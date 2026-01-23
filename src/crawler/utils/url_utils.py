# src/crawler/utils/url_utils.py
import os
import logging
from urllib.parse import urlparse, urljoin, urlunparse

logger = logging.getLogger(__name__)


class UrlUtils:
    """A collection of static methods for URL parsing and manipulation."""

    @staticmethod
    def normalize_url(base_url: str, url: str) -> str:
        """
        Creates a clean, absolute URL from a core URL and a potentially relative URL.
        """
        if isinstance(base_url, bytes):
            base_url = base_url.decode('utf-8')
        if isinstance(url, bytes):
            url = url.decode('utf-8')

        absolute_url = urljoin(base_url, url)
        parsed_url = urlparse(absolute_url)

        # Ensure there is a path (e.g., '/' for the homepage)
        if not parsed_url.path:
            parsed_url = parsed_url._replace(path='/')

        # Remove fragments, as they are client-side only
        parsed_url = parsed_url._replace(fragment='')

        return urlunparse(parsed_url)

    @staticmethod
    def get_base_url(url: str) -> str | None:
        """
        Extracts and returns the core URL (scheme + netloc) from a given URL.
        """
        if isinstance(url, bytes):
            url = url.decode('utf-8')

        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url

        try:
            parsed_url = urlparse(url)
            if not parsed_url.scheme or not parsed_url.netloc:
                logger.debug(f"Invalid URL format: {url}")
                return None
            return f"{parsed_url.scheme}://{parsed_url.netloc}"
        except ValueError:
            logger.debug(f"Could not parse invalid URL: {url}")
            return None

    @staticmethod
    def is_relative_url(url: str) -> bool:
        """
        Checks if a URL is relative.
        """
        try:
            parsed = urlparse(url)
            return not parsed.scheme and not parsed.netloc
        except ValueError:
            return False

    @staticmethod
    def is_allowed_extension(url: str) -> bool:
        """
        Checks if a URL's extension is on the whitelist of crawlable page types.
        URLs without an extension are considered valid.
        """

        """
        Extensive whitelist based on rigours research. 
        The "" string is crucial for urls without extension.
        """
        WHITELISTED_EXTENSIONS = [
            "", ".htm", ".html", ".xhtml", ".shtml", ".shtm", ".stm",
            ".jhtml", ".asp", ".aspx", ".ashx", ".asmx", ".axd", ".mspx",
            ".jsp", ".jspx", ".do", ".action", ".jsf", ".faces",
            ".php", ".php3", ".php4", ".php5", ".phtml",
            ".pl", ".cgi", ".fcgi", ".py", ".rb", ".rhtml", ".dll",
            ".cfm", ".cfml", ".yaws", ".lasso", ".nsf", ".xsp", ".hcsp", ".adp"
        ]

        try:
            path = urlparse(url).path
            _, extension = os.path.splitext(path)

            return extension.lower() in WHITELISTED_EXTENSIONS
        except Exception:
            return False

    @staticmethod
    def is_valid_link(url: str, allow_query_params: bool = False, allow_fragments: bool = False) -> bool:
        """
        Checks if a URL is a valid, fetchable web URL using a whitelist for extensions.
        """
        if not isinstance(url, str):
            logger.debug(f"Invalid link check: URL is not a string ({type(url).__name__}).")
            return False

        try:
            parsed_url = urlparse(url)

            # 1. Check for valid scheme (http or https)
            if parsed_url.scheme not in ['http', 'https']:
                return False

            # 2. Check if a 'hostname' exists
            if not parsed_url.netloc:
                return False

            # 3. Check for allowed file extensions using specialist function
            if not UrlUtils.is_allowed_extension(url):
                logger.debug(f"Invalid link check: Extension not in whitelist for URL '{url}'.")
                return False

            # 4. Check for query parameters (optional)
            if not allow_query_params and parsed_url.query:
                return False

            # 5. Check for URL fragments (optional)
            if not allow_fragments and parsed_url.fragment:
                return False

        except ValueError as e:
            logger.debug(f"Invalid link check: ValueError during URL parsing for '{url}': {e}.")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during URL validation for '{url}': {e}", exc_info=True)
            return False

        return True

    @staticmethod
    def is_internal_link(url: str, base_url: str) -> bool:
        """
        Checks if a URL is internal relative to the base_url.
        """
        try:
            return urlparse(url).netloc == urlparse(base_url).netloc
        except ValueError:
            return False

    # 👇 NEW METHOD (simplified implementation for PydPiper Mini)
    @staticmethod
    def is_canonical_page(url: str, base_url: str) -> bool:
        """
        Checks if a URL is a 'canonical' page we want to crawl.
        For PydPiper Mini, this is the same as a valid, internal link.
        """
        return UrlUtils.is_internal_link(url, base_url) and UrlUtils.is_valid_link(url)