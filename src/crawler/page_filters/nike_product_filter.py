import logging
from bs4 import BeautifulSoup
from crawler.page_filters.page_filter_base import PageFilterBase

logger = logging.getLogger(__name__)

class NikeProductFilter(PageFilterBase):
    """
    Detecteert Nike Product Detail Pages (PDP) tijdens de crawl.
    Zorgt voor 100% yield door alleen pagina's met een product_title te indexeren.
    """

    def __init__(self, soup: BeautifulSoup):
        super().__init__(soup)

    def apply(self) -> bool:
        """
        Controleert of de pagina de stabiele Nike PDP-kenmerken heeft.
        """
        logger.debug("NikeProductFilter toepassen...")

        # Methode 1: De stabiele 'product_title' selector (meest betrouwbaar)
        if self.soup.select_one('h1[data-testid="product_title"]'):
            logger.debug("NikeProductFilter: Match gevonden! Pagina is een PDP.")
            return True

        # Methode 2: Check Next.js JSON data (als fallback/dubbelcheck)
        # Soms is de HTML traag, maar de data zit altijd in __NEXT_DATA__
        next_data = self.soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            if '"pageType":"pdp"' in next_data.string or '"/t/' in next_data.string:
                logger.debug("NikeProductFilter: Match gevonden via __NEXT_DATA__.")
                return True

        logger.debug("NikeProductFilter: Geen productpagina. Pagina wordt genegeerd.")
        return False