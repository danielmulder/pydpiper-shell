import asyncio
import logging
import re
import io
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup

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
from pydpiper_shell.core.filter_registry import filter_registry
from pydpiper_shell.core.filter_registry import register_all_filters

logger = logging.getLogger(__name__)


class AsyncCrawlController(AsyncController):
    """
    High-Performance Async Crawler met Sitemap-ondersteuning en Page Filtering.
    """

    def __init__(
            self,
            project_id: int,
            start_url: str,
            run_mode: str,
            db_manager: DatabaseManager,
            cache_manager: Optional[Any] = None,
            config: Optional[Dict] = None,
            strict_mode: bool = True,
            respect_robots_txt: bool = False,
            page_filter_name: Optional[str] = None,
    ):
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

        # Page Filter Laden
        self.page_filter_class = None
        if page_filter_name:
            self.page_filter_class = filter_registry.get_filter(page_filter_name)
            if self.page_filter_class:
                logger.info(f"Page filter geactiveerd: {page_filter_name}")

        self.page_fetcher = PageFetcher(
            config=self.config,
            url_utils=self.url_utils,
            user_agent=self.user_agent
        )
        self.link_processor = LinkProcessorService()
        self.robots_txt_service: Optional[RobotsTxtService] = None

        # State & Buffers
        self.pages_crawled = 0
        self.request_failures = 0
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.visited: set[str] = set()

        self.pages_buffer: List[Page] = []
        self.internal_links_buffer: List[Link] = []
        self.external_links_buffer: List[Link] = []
        self.requests_buffer: List[Request] = []

        # Config van config_manager
        self.flush_interval = config_manager.get_nested("crawler.flush_interval", 10)
        self.skip_save = config_manager.get_nested("crawler.skip_save", False)
        self.respect_robots_meta_tags = config_manager.get_nested("crawler.robots_meta_tags", True)
        self.respect_nofollow = config_manager.get_nested("crawler.link_nofollow", True)

        self.stop_crawl_event = asyncio.Event()
        self.flush_lock = asyncio.Lock()
        self.crawl_count_lock = asyncio.Lock()

    async def _process_sitemap(self, xml_content: str, source_url: str):
        """
        Extraheert URL's uit een XML sitemap en voegt ze toe aan de in-memory queue.
        """
        try:
            # Namespace voor sitemaps (vrijwel altijd standaard)
            ns = '{http://www.sitemaps.org/schemas/sitemap/0.9}'
            stream = io.BytesIO(xml_content.encode('utf-8'))
            newly_queued = 0

            for event, elem in ET.iterparse(stream, events=('end',)):
                if elem.tag == f'{ns}loc':
                    loc = elem.text.strip()
                    if loc not in self.visited:
                        self.visited.add(loc)
                        await self.queue.put(loc)
                        newly_queued += 1
                elem.clear()

            if newly_queued > 0:
                logger.info(f"Sitemap parsed: {newly_queued} URL's toegevoegd vanuit {source_url}")
                await self._update_progress_bar_total()
        except Exception as e:
            logger.error(f"Fout bij parsen sitemap {source_url}: {e}")

    async def _process_url(self, url: str) -> Dict[str, Any]:
        if self.stop_crawl_event.is_set():
            return {"status": 0, "info": "Stopped"}

        try:
            # Fetch de pagina
            fetch_result = await self.page_fetcher.fetch_page(url)
            self._log_request(url, fetch_result)

            status = fetch_result.get("status")
            content = fetch_result.get("content")
            headers = fetch_result.get("headers", {})

            if status == 200 and content:
                # 1. Sitemap Detectie (XML content of URL)
                is_xml = "xml" in headers.get('Content-Type', '').lower() or url.endswith(".xml")
                if is_xml:
                    await self._process_sitemap(content, url)
                    return fetch_result

                # 2. Page Filter Toepassen (indien aanwezig)
                if self.page_filter_class:
                    soup = BeautifulSoup(content, 'html.parser')
                    if not self.page_filter_class(soup).apply():
                        logger.debug(f"Filter: Pagina {url} genegeerd.")
                        return {"status": 0, "info": "Filtered"}

                # 3. Product Pagina Opslaan
                async with self.crawl_count_lock:
                    self.pages_crawled += 1
                    self.pages_buffer.append(Page(url=url, status_code=status, content=content))

                    if self.progress_manager:
                        self.progress_manager.advance(pages_count=self.pages_crawled,
                                                      failures_count=self.request_failures)

                    if self.max_pages_to_crawl and self.pages_crawled >= self.max_pages_to_crawl:
                        self.stop_crawl_event.set()

                # 4. Links verwerken (Database opslag ja, Queue vullen hangt af van run_mode)
                await self._process_links(url, content)

            else:
                async with self.crawl_count_lock:
                    self.request_failures += 1

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return {"status": -99, "error": str(e)}

        return fetch_result

    async def _process_links(self, source_url: str, html_content: str) -> None:
        """Extraheert links. In sitemap-mode vullen we de queue NIET."""
        try:
            (internal, external) = self.link_processor.process_links(html_content, source_url, self.project_id)

            if internal:
                internal_objs = []
                for link_data in internal:
                    link_obj = Link(**link_data)
                    internal_objs.append(link_obj)

                    # KEY: Alleen URL's aan de queue toevoegen als we in discovery mode zijn
                    if self.run_mode == "discovery":
                        t_url = str(link_obj.target_url)
                        if t_url not in self.visited and not self.stop_crawl_event.is_set():
                            self.visited.add(t_url)
                            await self.queue.put(t_url)

                self.internal_links_buffer.extend(internal_objs)

            if external:
                self.external_links_buffer.extend([Link(**ld) for ld in external])

            if self.run_mode == "discovery":
                await self._update_progress_bar_total()

        except Exception as e:
            logger.debug(f"Link processing error: {e}")

    # ... (Rest van de controller methodes zoals run, shutdown, etc.) ...

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