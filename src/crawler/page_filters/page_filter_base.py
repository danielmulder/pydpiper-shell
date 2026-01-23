# pydpiper_shell/core/page_filters/page_filter_base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from bs4 import BeautifulSoup

class PageFilterBase(ABC):
    """Interface (bluelogger.info) for all HTML-bound page filters."""

    def __init__(self, soup: BeautifulSoup):
        self.soup = soup

    @abstractmethod
    def apply(self) -> bool:  # True = keep the page
        """Apply the filter and determine whether the page should be saved."""
        raise NotImplementedError