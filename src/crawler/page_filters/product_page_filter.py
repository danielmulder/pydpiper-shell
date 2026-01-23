# pydpiper_shell/core/page_filters/product_page_filter.py
# src/pydpiper_shell/core/page_filters/product_page_filter.py

import logging
from bs4 import BeautifulSoup
from crawler.page_filters.page_filter_base import PageFilterBase

logger = logging.getLogger(__name__)


class ProductPageFilter(PageFilterBase):
    """Detects Magento 2 product pages via magento-init script tags."""

    def __init__(self, soup: BeautifulSoup):
        super().__init__(soup)

    def apply(self) -> bool:
        logger.info("Applying ProductPageFilter...")
        for tag in self.soup.find_all("script", attrs={"type": "text/x-magento-init"}):
            if tag.string and '"pageType":"catalog_product_view"' in tag.string:
                logger.debug("ProductPageFilter: Match found on 'catalog_product_view'. Page is kept.")
                return True

        logger.debug("ProductPageFilter: No match found. Page is ignored.")
        return False