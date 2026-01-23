import asyncio
import logging
import time
import random
from typing import Dict, Optional, List
from urllib.parse import urljoin

import aiohttp
from crawler.utils.url_utils import UrlUtils

logger = logging.getLogger(__name__)


class PageFetcherService:
    """
    Core class for managing asynchronous HTTP sessions.
    Implements a Circuit Breaker pattern AND Adaptive Concurrency Throttling (AIMD)
    WITH Self-Learning Ceiling.
    """

    def __init__(self, config: Dict, url_utils: UrlUtils, user_agent: str):
        self.config = config
        self.url_utils = url_utils
        self.user_agent = user_agent

        session_config = config.get('session', {})

        self.max_concurrency_cap = int(session_config.get('concurrency', 50))
        self.timeout = int(session_config.get('time_out', 30))
        self.max_redirects = int(session_config.get('max_redirects', 10))

        # Hard limit (safety net)
        self.semaphore = asyncio.Semaphore(self.max_concurrency_cap)
        self.session: Optional[aiohttp.ClientSession] = None

        # --- DYNAMIC CONCURRENCY STATE (AIMD) ---
        self._current_concurrency_limit = self.max_concurrency_cap
        self._active_requests = 0
        self._concurrency_condition = asyncio.Condition()

        # --- SELF LEARNING STATE ---
        self._failure_history: List[int] = []
        self.smart_max_cap = self.max_concurrency_cap

        # --- CIRCUIT BREAKER STATE ---
        self._circuit_breaker = asyncio.Event()
        self._circuit_breaker.set()

        self._consecutive_429 = 0
        self._429_threshold = int(session_config.get('429_threshold', 3))
        self._current_backoff = int(session_config.get('current_backoff', 3))
        self.max_backoff = int(session_config.get('max_backoff', 12))
        self.up_damping_factor = float(session_config.get('up_damping_factor', 0.025))

        self._handling_429_lock = asyncio.Lock()

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def initialize(self):
        if not self.session or self.session.closed:
            # TCP connector tuning
            connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
            timeout_obj = aiohttp.ClientTimeout(total=self.timeout)
            default_headers = {
                'Accept-Encoding': 'gzip, deflate',
                'User-Agent': self.user_agent
            }
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout_obj,
                headers=default_headers
            )
            logger.debug(f"Fetch Service initialized. Max Concurrency: {self.max_concurrency_cap}")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def _acquire_slot(self):
        """
        Wait until space is available within the DYNAMIC limit.
        """
        async with self._concurrency_condition:
            while (self._active_requests >= self._current_concurrency_limit) or (not self._circuit_breaker.is_set()):
                if not self._circuit_breaker.is_set():
                    await self._circuit_breaker.wait()
                else:
                    await self._concurrency_condition.wait()
            self._active_requests += 1

    async def _release_slot(self):
        """
        Release a slot and wake up waiting workers.
        """
        async with self._concurrency_condition:
            self._active_requests -= 1
            self._concurrency_condition.notify()

    def _update_smart_ceiling(self, crash_point: int):
        """
        Calculates the average of previous crash points and sets a new safe ceiling.
        """
        self._failure_history.append(crash_point)
        avg_failure = sum(self._failure_history) / len(self._failure_history)
        new_cap = max(3, int(avg_failure))
        self.smart_max_cap = min(new_cap, self.max_concurrency_cap)

        if self.smart_max_cap <= 0:
            self.smart_max_cap = 1

        logger.warning(
            f"🧠 Self-Learning: Crash at {crash_point}. History: {self._failure_history[-5:]}. "
            f"New Smart Ceiling set to: {self.smart_max_cap}"
        )

    async def _adjust_concurrency_down(self):
        """
        Multiplicative Decrease + Update Learning Ceiling
        """
        async with self._concurrency_condition:
            old_limit = self._current_concurrency_limit

            # Learn from the failure
            self._update_smart_ceiling(old_limit)

            # Halve the limit (Emergency stop)
            self._current_concurrency_limit = max(1, int(self._current_concurrency_limit * 0.5))

            # Ensure we don't exceed the new smart ceiling immediately
            self._current_concurrency_limit = min(self._current_concurrency_limit, self.smart_max_cap)

            if old_limit != self._current_concurrency_limit:
                logger.warning(
                    f"📉 Throttling Down: Too many 429s. Concurrency dropped from {old_limit} to {self._current_concurrency_limit}."
                )

    async def _adjust_concurrency_up(self):
        """
        Additive Increase (respecting the new smart ceiling)
        """
        if self._active_requests < self._current_concurrency_limit:
            return

        # Check against the smart ceiling instead of the absolute ceiling
        if self._current_concurrency_limit < self.smart_max_cap:
            if random.random() < self.up_damping_factor:
                self._current_concurrency_limit += 1
                logger.info(
                    f"📈 Throttling Up: Limit increased to {self._current_concurrency_limit} (Cap: {self.smart_max_cap})"
                )

    async def _trigger_429_protection(self):
        """
        Called on a 429 status code.
        Handles concurrency backoff and circuit breaker logic within a lock
        to prevent 'stampede' effects.
        """
        if self._handling_429_lock.locked():
            return

        async with self._handling_429_lock:
            if not self._circuit_breaker.is_set():
                return

            # FIRST: Scale down concurrency (the structural solution)
            await self._adjust_concurrency_down()

            self._consecutive_429 += 1

            # Only if we fail repeatedly, perform emergency stop (Circuit Breaker)
            if self._consecutive_429 >= self._429_threshold:
                self._circuit_breaker.clear()
                wait_time = self._current_backoff
                self._current_backoff = min(self._current_backoff * 1.5, self.max_backoff)

                logger.warning(
                    f"⛔ Circuit Breaker TRIPPED. Pausing all {self._active_requests} active workers for {wait_time}s."
                )
                await asyncio.sleep(wait_time)

                self._consecutive_429 = 0
                self._circuit_breaker.set()

                async with self._concurrency_condition:
                    self._concurrency_condition.notify_all()

                logger.info("✅ Circuit Breaker RESET. Resuming.")


class PageFetcher(PageFetcherService):
    def __init__(self, config: Dict, url_utils: UrlUtils, user_agent: str):
        super().__init__(config, url_utils, user_agent)
        self.url: Optional[str] = None
        self.base_url: Optional[str] = None

    async def fetch_page(self, url_to_fetch: str) -> dict:
        start_total_time = time.perf_counter()
        self.url = url_to_fetch
        self.base_url = self.url_utils.get_base_url(self.url)

        timers = {}
        response_data = None

        if not self.session or self.session.closed:
            await self.initialize()

        # --- STEP 1: SMART GATEKEEPER ---
        await self._acquire_slot()

        try:
            async with self.semaphore:
                # Jitter at low concurrency
                if self._current_concurrency_limit < 5:
                    await asyncio.sleep(random.uniform(0.1, 0.5))

                timeout_val = float(self.config.get('session', {}).get('fetch_page_total_timeout', 30.0))

                async with self.session.get(
                        url_to_fetch,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=timeout_val)
                ) as response:
                    status = response.status

                    # --- STEP 2: FEEDBACK LOOP ---
                    if status == 429:
                        asyncio.create_task(self._trigger_429_protection())
                    elif status == 200:
                        await self._adjust_concurrency_up()
                        self._current_backoff = max(2.0, self._current_backoff * 0.9)
                        self._consecutive_429 = 0

                    headers = dict(response.headers)
                    content = None
                    redirect_chain = []
                    timers["initial_request"] = round((time.perf_counter() - start_total_time) * 1000, 2)

                    # --- CONTENT TYPE CHECK (Asset Handling) ---
                    content_type = headers.get("Content-Type", "").lower()

                    # List of types we accept as 'Valid' (200 OK)
                    # but where we don't need to store the body.
                    ALLOWED_NON_HTML = [
                        'application/pdf',
                        'application/xml',
                        'text/xml',
                        'application/json',
                        'application/octet-stream'  # Often used for downloads
                    ]

                    is_html = "text/html" in content_type
                    is_allowed_asset = any(t in content_type for t in ALLOWED_NON_HTML)

                    if status == 200:
                        if is_html:
                            # Normal HTML: Download body
                            content = await self._read_content(response, timers)
                        elif is_allowed_asset:
                            # Valid asset: Accept as 200, but no body.
                            # Prevents status -10 and saves memory/CPU.
                            content = None
                            logger.debug(f"Asset detected ({content_type}): {self.url}. Saving metadata without body.")
                        else:
                            # Unknown/Unwanted type: Mark as -10 (Error)
                            response_data = {
                                "status": -10,
                                "headers": headers,
                                "content": None,
                                "redirect_chain": [],
                                "timers": timers,
                                "error": f"Unsupported Content-Type: {content_type}"
                            }
                            return response_data

                    is_redirect = status in (301, 302, 303, 307, 308)
                    if is_redirect:
                        redirect_start = time.perf_counter()
                        redirect_chain = await self.follow_redirects(self.url)
                        timers["follow_redirects"] = round(
                            (time.perf_counter() - redirect_start) * 1000, 2
                        )

                    response_data = {
                        "status": status, "headers": headers, "content": content,
                        "redirect_chain": redirect_chain, "timers": timers
                    }

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            response_data = {"status": -1, "error": str(e)}
        except Exception as e:
            logger.error(f"Internal fetch error for {url_to_fetch}: {e}", exc_info=True)
            response_data = {"status": -2, "error": str(e)}
        finally:
            await self._release_slot()

            if response_data:
                total_elapsed = round((time.perf_counter() - start_total_time), 4)
                response_data["elapsed_time"] = total_elapsed
                response_data.setdefault("timers", {})["total_elapsed"] = round(
                    total_elapsed * 1000, 2
                )

        return response_data if response_data else {"status": -99, "error": "Unknown failure"}

    async def _read_content(self, response, timers) -> Optional[str]:
        read_start = time.perf_counter()
        content = None
        try:
            read_timeout = float(
                self.config.get('session', {}).get('client_read_timeout', 5.0)
            )
            content = await asyncio.wait_for(response.text(), timeout=read_timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout reading response for %s", self.url)
        except UnicodeDecodeError:
            content_bytes = await response.read()
            content = content_bytes.decode('utf-8', errors='replace')
        finally:
            timers["read_content"] = round(
                (time.perf_counter() - read_start) * 1000, 2
            )
        return content

    async def follow_redirects(self, initial_url: str) -> list:
        if not self.session:
            return [{'error': 'Session not active', 'url': initial_url}]

        redirect_chain = []
        visited = {initial_url}
        current_url = initial_url
        timeout_per_redirect = int(
            self.config.get('session', {}).get('max_timeout_redirects', 5)
        )

        for _ in range(self.max_redirects):
            if not self._circuit_breaker.is_set():
                await self._circuit_breaker.wait()

            try:
                async with self.session.head(
                        current_url,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=timeout_per_redirect)
                ) as response:

                    if response.status == 429:
                        asyncio.create_task(self._trigger_429_protection())

                    location = response.headers.get('location')
                    if response.status in (301, 302, 303, 307, 308) and location:
                        next_url = urljoin(current_url, location)
                        redirect_chain.append(
                            {'source': current_url, 'target': next_url, 'status': response.status}
                        )
                        if next_url in visited:
                            redirect_chain.append({'error': 'Redirect loop', 'url': next_url})
                            break
                        visited.add(next_url)
                        current_url = next_url
                    else:
                        break
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                redirect_chain.append({'error': str(e), 'url': current_url})
                break
        return redirect_chain