# src/crawler/services/robots_txt_service.py
import asyncio
import logging
import urllib.robotparser
from typing import Dict
from urllib.parse import urljoin, urlparse

import aiohttp

logger = logging.getLogger(__name__)


class RobotsTxtService:
    """
    Manages fetching, parsing, and caching of robots.txt files.
    """

    def __init__(self, session: aiohttp.ClientSession, user_agent: str):
        """
        Initializes the service.

        Args:
            session: The shared aiohttp.ClientSession for requests.
            user_agent: The User-Agent string for fetching and checks.
        """
        self._session = session
        self._user_agent = user_agent
        self._parser_cache: Dict[str, urllib.robotparser.RobotFileParser] = {}
        self._fetch_locks: Dict[str, asyncio.Lock] = {}

    async def _get_parser(self, base_url: str) -> urllib.robotparser.RobotFileParser:
        """
        Retrieves a parsed RobotFileParser for a domain, fetching if not
        cached. This method ensures a domain's robots.txt is fetched only once.
        """
        if base_url in self._parser_cache:
            return self._parser_cache[base_url]

        if base_url not in self._fetch_locks:
            self._fetch_locks[base_url] = asyncio.Lock()

        async with self._fetch_locks[base_url]:
            if base_url in self._parser_cache:
                return self._parser_cache[base_url]

            robots_url = urljoin(base_url, "/robots.txt")
            parser = urllib.robotparser.RobotFileParser(url=robots_url)

            try:
                async with self._session.get(robots_url, timeout=10) as response:
                    if 200 <= response.status < 300:
                        content = await response.text()
                        parser.parse(content.splitlines())
                        logger.debug("Fetched and parsed robots.txt for %s", base_url)
                    else:
                        logger.debug(
                            "robots.txt not found for %s (status: %d). Allowing all.",
                            base_url, response.status
                        )
            except Exception as e:
                logger.warning(
                    "Could not fetch robots.txt for %s: %s. Allowing all.", base_url, e
                )

            self._parser_cache[base_url] = parser
            return parser

    async def can_fetch(self, url: str) -> bool:
        """
        Checks if the crawler is allowed to fetch a URL based on rules.

        Args:
            url: The full URL to check.

        Returns:
            True if fetching is allowed, False otherwise.
        """
        try:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        except Exception:
            return False

        parser = await self._get_parser(base_url)
        can_fetch = parser.can_fetch(self._user_agent, url)
        if not can_fetch:
            logger.debug(f"Can fetch ruling for url {parsed_url}: {can_fetch}")
        return can_fetch