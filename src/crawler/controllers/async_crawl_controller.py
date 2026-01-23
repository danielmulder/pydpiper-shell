import asyncio
import logging
import re
from typing import Optional, List, Dict, Any

from pydantic import ValidationError

from crawler.utils.url_utils import UrlUtils
from crawler.utils.run_timers import RunTimers
from crawler.model import Page, Link, Request, CrawlSettings
from crawler.controllers.async_controller import AsyncController
from crawler.managers.adaptive_worker_manager import AdaptiveWorkerManager
from crawler.managers.crawl_data_manager import CrawlDataManager
from crawler.managers.progress_manager import ProgressManager
from crawler.services.async_page_fetcher_service import PageFetcher
from crawler.services.generate_default_user_agent_service import generate_default_user_agent
from crawler.services.link_processor_service import LinkProcessorService
from crawler.services.robots_txt_service import RobotsTxtService
from pydpiper_shell.core.managers.config_manager import config_manager
from pydpiper_shell.core.managers.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class AsyncCrawlController(AsyncController):
    """
    High-Performance Async Crawler Controller.

    This controller orchestrates the crawling process using a hybrid approach:
    1.  **Fast Engine:** Uses `PageFetcher` (aiohttp) for high-concurrency downloads.
    2.  **DB Persistence:** Asynchronously saves data to SQLite via `CrawlDataManager`.
    3.  **Turbo Logic:** Uses compiled Regex for meta-tag checks to avoid costly DOM parsing.

    It manages the URL queue, worker tasks, data buffering, and periodic database flushing.
    """

    def __init__(
            self,
            project_id: int,
            start_url: str,
            run_mode: str,
            db_manager: DatabaseManager,
            cache_manager: Optional[Any] = None,  # For signature compatibility
            config: Optional[Dict] = None,
            strict_mode: bool = True,
            respect_robots_txt: bool = False,
            page_filter_name: Optional[str] = None,
    ):
        """
        Initialize the controller with project settings and managers.

        Args:
            project_id: The ID of the current project.
            start_url: The entry point URL for the crawl.
            run_mode: The execution mode (e.g., 'discovery', 'sitemap').
            db_manager: The database manager instance for persistence.
            cache_manager: Legacy cache manager (unused, kept for compatibility).
            config: Optional configuration dictionary.
            strict_mode: If True, enforces stricter validation on links.
            respect_robots_txt: If True, checks robots.txt before fetching.
            page_filter_name: Optional name of a page filter to apply.
        """
        super().__init__()
        self.config = config or {}
        self.project_id = project_id
        self.start_url = start_url
        self.run_mode = run_mode
        self.strict_mode = strict_mode
        self.respect_robots_txt = respect_robots_txt

        # Managers & Services
        self.db = CrawlDataManager(delegate=db_manager)
        self.timer = RunTimers()
        self.url_utils = UrlUtils()
        self.user_agent = generate_default_user_agent()

        self.page_fetcher = PageFetcher(
            config=self.config,
            url_utils=self.url_utils,
            user_agent=self.user_agent
        )
        self.link_processor = LinkProcessorService()
        self.robots_txt_service: Optional[RobotsTxtService] = None
        self.worker_manager: Optional[AdaptiveWorkerManager] = None
        self.progress_manager: Optional[ProgressManager] = None

        # Configuration
        self.max_pages_to_crawl: Optional[int] = None
        self.flush_interval = config_manager.get_nested("crawler.flush_interval", 10)
        self.skip_save = config_manager.get_nested("crawler.skip_save", False)
        self.respect_robots_meta_tags = config_manager.get_nested("crawler.robots_meta_tags", True)
        self.respect_nofollow = config_manager.get_nested("crawler.link_nofollow", True)

        # Regex patterns for Turbo Meta Checks (Pre-compiled for performance)
        # Matches: <meta ... name="robots" ... content="...noindex...">
        self._meta_robots_pattern = re.compile(
            r'<meta[^>]+name=["\']robots["\'][^>]*content=["\'][^"\']*noindex[^"\']*["\']',
            re.IGNORECASE | re.DOTALL
        )
        # Matches: <meta ... content="...noindex..." ... name="robots">
        self._meta_robots_pattern_alt = re.compile(
            r'<meta[^>]+content=["\'][^"\']*noindex[^"\']*["\'][^>]*name=["\']robots["\']',
            re.IGNORECASE | re.DOTALL
        )

        # State & Buffers
        self.pages_crawled = 0
        self.request_failures = 0
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.visited: set[str] = set()

        self.pages_buffer: List[Page] = []
        self.internal_links_buffer: List[Link] = []
        self.external_links_buffer: List[Link] = []
        self.requests_buffer: List[Request] = []

        # Concurrency Control
        self._flusher_task: Optional[asyncio.Task] = None
        self.flush_lock = asyncio.Lock()
        self.crawl_count_lock = asyncio.Lock()

    def _is_noindex(self, content: str) -> bool:
        """
        TURBO CHECK: Scans only the first ~5KB of the HTML for noindex tags.
        Regex is significantly faster than BeautifulSoup for this specific check.

        Args:
            content: The HTML content string.

        Returns:
            True if a 'noindex' directive is found, False otherwise.
        """
        if not content:
            return False

        # We only check the head region (first 5120 characters are usually sufficient)
        head_sample = content[:5120]

        if self._meta_robots_pattern.search(head_sample):
            return True
        if self._meta_robots_pattern_alt.search(head_sample):
            return True

        return False

    async def _flush_buffer(self) -> None:
        """
        Atomically flushes all data buffers to the database via a thread pool.
        This ensures database I/O does not block the main asyncio event loop.
        """
        async with self.flush_lock:
            try:
                loop = asyncio.get_running_loop()
                buffers_to_flush = [
                    ("pages", self.pages_buffer),
                    ("internal_links", self.internal_links_buffer),
                    ("external_links", self.external_links_buffer),
                    ("requests", self.requests_buffer),
                ]
                tasks = []

                for name, buf in buffers_to_flush:
                    if buf:
                        # Handle dry run mode
                        if self.skip_save:
                            buf.clear()
                            continue

                        data_copy = buf.copy()
                        buf.clear()

                        # Use a closure/wrapper to bind arguments correctly for the thread executor
                        def save_task(pid=self.project_id, n=name, d=data_copy):
                            try:
                                logger.debug(f"saving with: {n}")
                                self.db.save(pid, n, d)
                            except Exception as e:
                                logger.error(f"DB Save error for {n}: {e}")

                        # Offload to thread pool
                        tasks.append(loop.run_in_executor(None, save_task))

                if tasks:
                    await asyncio.gather(*tasks)
            except RuntimeError:
                # Loop might be closed during shutdown
                return
            except Exception as e:
                logger.error("Error flushing buffer: %s", e, exc_info=True)

    async def _periodic_flush_task(self, interval_seconds: int) -> None:
        """
        Background task that triggers a buffer flush at set intervals.

        Args:
            interval_seconds: Time in seconds between flush attempts.
        """
        while not self.stop_crawl_event.is_set():
            try:
                await asyncio.sleep(interval_seconds)
                # Only flush if there is data
                if any([self.pages_buffer, self.internal_links_buffer,
                        self.external_links_buffer, self.requests_buffer]):
                    await self._flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Periodic flush error: %s", e)

    async def _process_url(self, url: str) -> Dict[str, Any]:
        """
        Fetches and processes a single URL.
        Designed for maximum throughput with minimal blocking logic.

        Args:
            url: The URL to process.

        Returns:
            A dictionary containing the fetch result and status.
        """
        if self.stop_crawl_event.is_set():
            return {"status": 0, "info": "Stopped"}

        try:
            # 1. Robots.txt check (Lightweight)
            if self.respect_robots_txt and self.robots_txt_service:
                if not await self.robots_txt_service.can_fetch(url):
                    return {"status": 0, "info": "Disallowed"}

            # 2. URL Validation
            if not self.url_utils.is_valid_link(url):
                async with self.crawl_count_lock:
                    self.request_failures += 1
                return {"status": -1, "error": "Invalid link"}

            # 3. High-Speed Fetch
            fetch_result = await self.page_fetcher.fetch_page(url)
            self._log_request(url, fetch_result)

            status = fetch_result.get("status")
            content = fetch_result.get("content")

            if status == 200 and content:

                # --- TURBO META ROBOTS CHECK ---
                # Performs a fast regex scan instead of full DOM parsing
                if self.respect_robots_meta_tags:
                    if self._is_noindex(content):
                        logger.debug("Skipping %s: NoIndex meta tag found (Regex).", url)
                        return {"status": 0, "info": "Skipped (NoIndex)"}
                # -------------------------------

                # Save Logic
                async with self.crawl_count_lock:
                    if (self.max_pages_to_crawl is None or
                            self.pages_crawled < self.max_pages_to_crawl):
                        self.pages_crawled += 1
                        self.pages_buffer.append(
                            Page(url=url, status_code=status, content=content)
                        )

                        if self.progress_manager:
                            self.progress_manager.advance(
                                pages_count=self.pages_crawled,
                                failures_count=self.request_failures
                            )

                        if (self.max_pages_to_crawl is not None and
                                self.pages_crawled >= self.max_pages_to_crawl):
                            logger.info("Max pages limit reached.")
                            self.stop_crawl_event.set()
                    else:
                        return fetch_result

                # Link Processing (Only if still running and not in sitemap mode)
                if not self.stop_crawl_event.is_set() and self.run_mode != "sitemap":
                    await self._process_links(url, content)

            else:
                async with self.crawl_count_lock:
                    self.request_failures += 1

        except Exception as e:
            logger.error("Error processing %s: %s", url, e)
            async with self.crawl_count_lock:
                self.request_failures += 1
            return {"status": -99, "error": str(e)}

        return fetch_result

    async def _process_links(self, source_url: str, html_content: str) -> None:
        """
        Extracts links from HTML, applies nofollow logic, and queues valid URLs.

        Args:
            source_url: The URL of the page where links were found.
            html_content: The raw HTML content of the page.
        """
        newly_queued = 0
        try:
            (internal, external) = self.link_processor.process_links(
                html_content=html_content,
                source_url=source_url,
                project_id=self.project_id
            )

            # --- PROCESS INTERNAL LINKS ---
            if internal:
                internal_objs = []
                for link_data in internal:
                    try:
                        link_obj = Link(**link_data)

                        # --- TURBO NOFOLLOW CHECK ---
                        # Checks if 'nofollow' exists in the rel attribute string representation
                        is_nofollow = False
                        if self.respect_nofollow and link_obj.rel:
                            if "nofollow" in str(link_obj.rel).lower():
                                is_nofollow = True

                        # Buffer for DB storage regardless of nofollow status (for audit purposes)
                        internal_objs.append(link_obj)

                        # Queue only if NOT nofollow and unique
                        if not is_nofollow:
                            t_url = str(link_obj.target_url)
                            if not self.stop_crawl_event.is_set() and t_url not in self.visited:
                                self.visited.add(t_url)
                                await self.queue.put(t_url)
                                newly_queued += 1

                    except ValidationError:
                        pass

                self.internal_links_buffer.extend(internal_objs)

            # --- PROCESS EXTERNAL LINKS ---
            if external:
                self.external_links_buffer.extend(
                    [Link(**ld) for ld in external if isinstance(ld, dict)]
                )

            # Update progress bar total if running without a hard limit
            if newly_queued > 0:
                await self._update_progress_bar_total()

        except Exception as e:
            logger.debug("Link processing error for %s: %s", source_url, e)

    async def _update_progress_bar_total(self) -> None:
        """Dynamically adjusts the progress bar total based on discovered links."""
        if not self.progress_manager or not self.progress_manager.pbar:
            return

        # Always show total unique URLs discovered, even if max_pages is set
        new_total = len(self.visited)
        current = self.progress_manager.pbar.n

        # Ensure total doesn't drop below current progress
        if new_total < current:
            new_total = current

        self.progress_manager.set_total(new_total)

    async def run(self, settings: CrawlSettings) -> None:
        """
        Main execution entry point for the crawl process.

        Args:
            settings: Runtime settings for the crawler (concurrency, limits, etc.).
        """
        self.timer.start()
        self.max_pages_to_crawl = settings.max_pages
        await self.page_fetcher.initialize()

        if self.respect_robots_txt and self.page_fetcher.session:
            self.robots_txt_service = RobotsTxtService(
                self.page_fetcher.session, self.user_agent
            )

        self._flusher_task = asyncio.create_task(
            self._periodic_flush_task(self.flush_interval)
        )

        # Seed the queue
        self.visited.add(str(self.start_url))
        await self.queue.put(str(self.start_url))

        # Start with 1 known URL. The bar will grow dynamically.
        initial_total = 1
        self.progress_manager = ProgressManager(
            total=initial_total,
            desc="Crawling",
            unit="url",
            max_pages=self.max_pages_to_crawl
        )

        # Start Worker Manager
        self.worker_manager = AdaptiveWorkerManager(
            work_coro=self._process_url,
            queue=self.queue,
            concurrency=settings.concurrency,
            stop_event=self.stop_crawl_event,
        )

        self._worker_task = asyncio.create_task(self.worker_manager.run())

        # Wait Strategy
        if self.max_pages_to_crawl is not None:
            # If limited, we wait for the stop event (limit reached).
            # We do NOT wait for queue.join(), because the queue might still contain
            # unscanned URLs that we don't intend to visit.
            await self.stop_crawl_event.wait()
        else:
            # If unlimited, we wait until the queue is naturally empty.
            await self.queue.join()
            self.stop_crawl_event.set()

        # Cleanup Tasks
        if self._worker_task and not self._worker_task.done():
            # Cancel the worker manager gracefully if it's still running
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        if self._flusher_task:
            self._flusher_task.cancel()

        logger.info("Performing final buffer flush...")
        await self._flush_buffer()

        if self.progress_manager:
            # Indicate if we were capped (limit reached)
            is_capped = (self.max_pages_to_crawl is not None and
                         self.pages_crawled >= self.max_pages_to_crawl)
            self.progress_manager.close(
                self.pages_crawled,
                self.request_failures,
                capped=is_capped
            )

        self.timer.stop()
        duration = self.timer.duration
        pps = self.pages_crawled / duration if duration > 0 else 0

        logger.info(
            f"Crawl finished. {self.pages_crawled} pages in {duration:.2f}s ({pps:.2f} p/s)."
        )

        await self.shutdown()

    async def shutdown(self) -> None:
        """Gracefully shuts down resources and closes connections."""
        if not self.stop_crawl_event.is_set():
            self.stop_crawl_event.set()

        await self.page_fetcher.close()

        logger.info("AsyncCrawlController shutdown complete.")

    def _log_request(self, url: str, fetch_result: Dict) -> None:
        """
        Logs request details to the buffer for auditing.

        Args:
            url: The requested URL.
            fetch_result: The dictionary containing response details.
        """
        try:
            req = Request(
                project_id=self.project_id,
                url=url,
                method="GET",
                status_code=fetch_result.get("status", -1),
                headers=fetch_result.get("headers", {}),
                elapsed_time=fetch_result.get("elapsed_time", 0.0),
                timers=fetch_result.get("timers", {}),
                redirect_chain=fetch_result.get("redirect_chain", [])
            )
            self.requests_buffer.append(req)
        except Exception as e:
            # Log error to file only, avoid console spam
            logger.error(f"Failed to log request for {url}: {e}")