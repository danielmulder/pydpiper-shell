# pydpiper_shell/core/page_filters/smart_product_filter.py
import logging
import json
from bs4 import BeautifulSoup
from crawler.page_filters.page_filter_base import PageFilterBase

logger = logging.getLogger(__name__)


class SmartProductFilter(PageFilterBase):
    """
    A smart, catch-all filter to identify product pages based on a series of
    common heuristics, from most reliable to least reliable.
    """

    def __init__(self, soup: BeautifulSoup):
        super().__init__(soup)

    def _has_product_json_ld(self) -> bool:
        """Checks for JSON-LD script tags with '@type': 'Product'."""
        for tag in self.soup.find_all("script", attrs={"type": "application/ld+json"}):
            if not tag.string:
                continue
            try:
                data = json.loads(tag.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict) and item.get("@type") in ("Product", "ProductGroup"):
                        return True
            except json.JSONDecodeError:
                continue
        return False

    def _has_product_og_type(self) -> bool:
        """Checks for <meta property="og:type" content="product">."""
        tag = self.soup.find("meta", property="og:type")
        return tag and tag.get("content", "").lower().strip() == "product"

    def _has_og_product_prefix(self) -> bool:
        """Checks for 'product: http://ogp.me/ns/product#' in <html prefix="...">."""
        html_tag = self.soup.find('html')
        if html_tag and 'prefix' in html_tag.attrs:
            return 'product: http://ogp.me/ns/product#' in html_tag['prefix']
        return False

    def _has_platform_body_class(self) -> bool:
        """Checks for common e-commerce platform classes on the <body> tag."""
        body = self.soup.find("body")
        if not body or not body.get("class"):
            return False

        platform_classes = {
            "catalog-product-view",  # Magento
            "single-product",  # WooCommerce
            "template-product",  # Shopify
        }

        body_classes = set(body.get("class", []))
        return not platform_classes.isdisjoint(body_classes)

    def _has_add_to_cart_button(self) -> bool:
        """Searches for buttons or inputs with 'add to cart' related text."""
        add_to_cart_texts = [
            "add to cart", "add to basket",
        ]

        for btn in self.soup.find_all("button"):
            if any(text in btn.get_text(strip=True).lower() for text in add_to_cart_texts):
                return True

        for inp in self.soup.find_all("input", attrs={"type": "submit"}):
            value = inp.get("value", "")
            if any(text in value.lower() for text in add_to_cart_texts):
                return True

        return False

    def apply(self) -> bool:
        """
        Applies all heuristics in order of reliability. Returns True on the first match.
        """
        if self._has_product_json_ld():
            logger.debug("SmartProductFilter: Match via JSON-LD.")
            return True

        if self._has_product_og_type():
            logger.debug("SmartProductFilter: Match via Open Graph meta tag.")
            return True

        if self._has_og_product_prefix():
            logger.debug("SmartProductFilter: Match via Open Graph HTML prefix.")
            return True

        if self._has_platform_body_class():
            logger.debug("SmartProductFilter: Match via platform body class.")
            return True

        if self._has_add_to_cart_button():
            logger.debug("SmartProductFilter: Match via 'Add to Cart' button.")
            return True

        logger.debug("SmartProductFilter: No product page indicators found.")
        return False