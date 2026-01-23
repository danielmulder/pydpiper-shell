# src/pydpiper_shell/core/filter_registry.py
import logging
from typing import Dict, Type
from pydpiper_shell.core.discovery import discover_filters
from crawler.page_filters.page_filter_base import PageFilterBase

logger = logging.getLogger(__name__)

# The central registry for all discovered page filters
FilterRegistry: Dict[str, Type[PageFilterBase]] = {}

def register_all_filters() -> None:
    """Discovers and registers all available page filters."""
    logger.debug("Discovering all page filters...")
    discovered_filters = discover_filters()
    FilterRegistry.update(discovered_filters)
    logger.debug("Successfully registered %d page filters.", len(FilterRegistry))