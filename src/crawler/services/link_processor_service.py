# src/crawler/services/link_processor_service.py

import logging
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.utils.url_utils import UrlUtils #

# Initialize a module-level logger.
logger = logging.getLogger(__name__)


class LinkProcessorService:
    """
    A stateless service responsible for parsing HTML content to
    extract, categorize, and normalize all anchor (<a>) tags.
    """

    def __init__(self):
        """
        Initializes the service by creating a utility class instance.
        The service itself remains stateless.
        """
        self.utils = UrlUtils() # Provides URL manipulation and validation utilities

    def process_links(
        self, html_content: str, source_url: str, project_id: int
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Processes all anchor tags on a page and categorizes them.

        Args:
            html_content: The raw HTML content of the page.
            source_url: The URL of the page the links originate from.
            project_id: The ID of the current project.

        Returns:
            A tuple containing two lists of dictionaries: (internal_links, external_links).
        """
        internal_links = []
        external_links = []

        base_url = self.utils.get_base_url(source_url) # Extract scheme://netloc
        if not base_url:
            logger.warning(f"Could not extract base_url from {source_url}. Cannot process links.")
            return [], []

        try:
            soup = BeautifulSoup(html_content, "html.parser")
            for link_tag in soup.find_all('a', href=True):
                raw_href = link_tag['href'].strip()

                # Skip anchors, mailto, tel, javascript protocols, etc.
                if not raw_href or raw_href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
                    continue

                # --- USE WHITELIST CHECK ---
                # First, create an absolute URL to check the extension correctly.
                absolute_target_url = urljoin(source_url, raw_href)
                # Check against the allowed extensions whitelist in UrlUtils.
                if not self.utils.is_allowed_extension(absolute_target_url): #
                    #logger.debug(f"Extension not allowed for: {absolute_target_url}. Skipping.")
                    continue
                # --- END WHITELIST CHECK ---

                # Normalize the URL only after the extension check passes.
                normalized_target_url = self.utils.normalize_url(base_url, absolute_target_url) # Clean up URL, remove fragment

                # Categorize the link based on whether it's internal and considered 'canonical' (crawlable).
                if self.utils.is_canonical_page(normalized_target_url, base_url): # Checks internal and valid link status
                    link_data = self._create_link_data(
                        link_tag, source_url, normalized_target_url, project_id
                    )
                    internal_links.append(link_data)
                elif self.utils.is_internal_link(normalized_target_url, base_url): # Checks if it belongs to the same domain
                    # Logic for non-canonical internal links (e.g., with parameters we might not crawl).
                    #logger.debug(f"Rejected normalized_target_url as non-canonical: {normalized_target_url}")
                    # No action needed currently, but could save separately in the future.
                    pass
                else:
                    # External links (extension check already passed).
                    link_data = self._create_link_data(
                        link_tag, source_url, normalized_target_url, project_id
                    )
                    external_links.append(link_data)

        except Exception as e:
            logger.error(f"Error processing links for {source_url}: {e}", exc_info=True)

        return internal_links, external_links

    def _create_link_data(self, link_tag, source_url, target_url, project_id) -> Dict:
        """Helper method to create a dictionary representing a link."""
        anchor_text = link_tag.get_text(strip=True) or ""
        rel_attr = link_tag.get("rel", "")
        # Handle cases where 'rel' might be a list of strings
        if isinstance(rel_attr, list):
            rel_attr = " ".join(rel_attr)

        return {
            "source_url": source_url,
            "target_url": target_url,
            "project_id": project_id,
            "status_code": 0,  # Placeholder, might be updated later if the link is crawled.
            "anchor": anchor_text,
            "rel": rel_attr
        }
