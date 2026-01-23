# src/crawler/services/http_request_service.py
import asyncio
import logging
import time
from typing import Dict, Optional

import aiohttp
from urllib.parse import urljoin

from crawler.utils.url_utils import UrlUtils

logger = logging.getLogger(__name__)


class HttpRequestService:
    """
    Central service for executing HTTP requests (GET/HEAD).
    Manages the aiohttp session, concurrency (semaphore), and error handling.
    """

    def __init__(self, config: Dict, url_utils: UrlUtils, user_agent: str):
        self.config = config
        self.url_utils = url_utils
        self.user_agent = user_agent

        session_config = config.get('session', {})
        self.max_concurrency = int(session_config.get('concurrency', 50))
        self.timeout = int(session_config.get('time_out', 30))
        self.max_redirects = int(session_config.get('max_redirects', 10))

        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def initialize(self):
        if not self.session or self.session.closed:
            timeout_obj = aiohttp.ClientTimeout(total=self.timeout)
            default_headers = {
                'Accept-Encoding': 'gzip, deflate',
                'User-Agent': self.user_agent
            }
            self.session = aiohttp.ClientSession(
                timeout=timeout_obj, headers=default_headers
            )
            logger.debug("HttpRequestService: Session initialized.")

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug("HttpRequestService: Session closed.")

    async def perform_request(self, url: str, method: str = "GET") -> dict:
        """
        Main entry point. Delegates to specific handlers based on method.
        Wraps execution in global semaphore and error handling.
        """
        start_time = time.perf_counter()

        if not self.session or self.session.closed:
            await self.initialize()
            if not self.session:
                return {"status": -99, "error": "Session not initialized"}

        response_data = None

        try:
            async with self.semaphore:
                # --- ROUTING LOGIC ---
                if method.upper() == "GET":
                    response_data = await self._execute_get(url, start_time)
                elif method.upper() == "HEAD":
                    response_data = await self._execute_head(url, start_time)
                else:
                    return {"status": -99, "error": f"Method {method} not supported"}
                # ---------------------

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            response_data = {"status": -1, "error": str(e)}
        except Exception as e:
            response_data = {"status": -2, "error": str(e)}
        finally:
            if response_data:
                # Calculate total elapsed time safely
                total_elapsed = round((time.perf_counter() - start_time), 4)
                response_data["elapsed_time"] = total_elapsed
                if "timers" not in response_data:
                    response_data["timers"] = {}
                response_data["timers"]["total_elapsed"] = round(total_elapsed * 1000, 2)

        return response_data if response_data else {"status": -99, "error": "Unknown failure"}

    # =========================================================================
    #  GET REQUEST LOGIC (Page Fetching)
    # =========================================================================
    async def _execute_get(self, url: str, start_time: float) -> dict:
        """
        Handles a full page fetch:
        1. Sends GET request.
        2. Follows redirects (if needed).
        3. Checks Content-Type (HTML only).
        4. Downloads and reads the body content.
        """
        base_url = self.url_utils.get_base_url(url)
        timeout_val = float(self.config.get('session', {}).get('fetch_page_total_timeout', 30.0))
        timers = {}

        async with self.session.get(
                url,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=timeout_val)
        ) as response:

            status = response.status
            headers = dict(response.headers)
            content = None
            redirect_chain = []

            timers["initial_request"] = round((time.perf_counter() - start_time) * 1000, 2)

            # 1. Handle Redirects
            if status in (301, 302, 303, 307, 308):
                redirect_start = time.perf_counter()
                redirect_chain = await self._follow_redirects(url)
                timers["follow_redirects"] = round((time.perf_counter() - redirect_start) * 1000, 2)

                # Take status of final destination if available
                if redirect_chain:
                    status = redirect_chain[-1].get('status', status)

            # 2. Handle Content (Only if 200 OK)
            elif status == 200:
                content_type = headers.get("Content-Type", "").lower()
                if "text/html" not in content_type:
                    logger.debug("Skipping non-HTML content for %s (%s)", url, content_type)
                    return {
                        "status": -10, "headers": headers, "content": None,
                        "redirect_chain": [], "timers": timers, "error": "Non-HTML Content-Type"
                    }

                # Read body
                content = await self._read_content(response, timers, url)

            return {
                "status": status,
                "headers": headers,
                "content": content,
                "redirect_chain": redirect_chain,
                "timers": timers,
                "final_url": str(response.url)
            }

    async def _read_content(self, response, timers, url) -> Optional[str]:
        """Helper to read response body text safely."""
        read_start = time.perf_counter()
        content = None
        try:
            read_timeout = float(self.config.get('session', {}).get('client_read_timeout', 15.0))
            content = await asyncio.wait_for(response.text(), timeout=read_timeout)
        except asyncio.TimeoutError:
            logger.warning("Timeout reading response body for %s", url)
        except UnicodeDecodeError:
            # Fallback decoding
            content_bytes = await response.read()
            content = content_bytes.decode('utf-8', errors='replace')
        finally:
            timers["read_content"] = round((time.perf_counter() - read_start) * 1000, 2)
        return content

    # =========================================================================
    #  HEAD REQUEST LOGIC (Link Checking)
    # =========================================================================
    async def _execute_head(self, url: str, start_time: float) -> dict:
        """
        Handles a lightweight link check:
        1. Sends HEAD request.
        2. Follows redirects (using HEAD).
        3. Returns status and headers (NO content download).
        """
        timeout_val = float(self.config.get('session', {}).get('fetch_page_total_timeout', 15.0))
        timers = {}

        async with self.session.head(
                url,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=timeout_val)
        ) as response:

            status = response.status
            headers = dict(response.headers)
            redirect_chain = []

            timers["initial_request"] = round((time.perf_counter() - start_time) * 1000, 2)

            # Handle Redirects
            if status in (301, 302, 303, 307, 308):
                redirect_start = time.perf_counter()
                redirect_chain = await self._follow_redirects(url)
                timers["follow_redirects"] = round((time.perf_counter() - redirect_start) * 1000, 2)

                if redirect_chain:
                    status = redirect_chain[-1].get('status', status)

            return {
                "status": status,
                "headers": headers,
                "content": None,  # Explicitly None for HEAD
                "redirect_chain": redirect_chain,
                "timers": timers,
                "final_url": str(response.url)
            }

    # =========================================================================
    #  SHARED HELPERS
    # =========================================================================
    async def _follow_redirects(self, initial_url: str) -> list:
        """
        Follows redirects using HEAD requests to minimize bandwidth.
        Shared by both GET and HEAD flows.
        """
        if not self.session:
            return [{'error': 'Session not active', 'url': initial_url}]

        redirect_chain = []
        visited = {initial_url}
        current_url = initial_url
        timeout_per_redirect = int(self.config.get('session', {}).get('max_timeout_redirects', 5))

        for _ in range(self.max_redirects):
            try:
                async with self.session.head(
                        current_url,
                        allow_redirects=False,
                        timeout=aiohttp.ClientTimeout(total=timeout_per_redirect)
                ) as response:
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
                        # End of chain
                        redirect_chain.append({'final_url': current_url, 'status': response.status})
                        break
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                redirect_chain.append({'error': str(e), 'url': current_url})
                break
        return redirect_chain