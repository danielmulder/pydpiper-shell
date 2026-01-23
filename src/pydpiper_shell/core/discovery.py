import importlib.util
import logging
from typing import Dict, Any, Tuple, Type
import inspect

from pydpiper_shell.core.utils.path_utils import PathUtils
from crawler.page_filters.page_filter_base import PageFilterBase

logger = logging.getLogger(__name__)


def discover_filters() -> Dict[str, Type[PageFilterBase]]:
    """
    Scans the page_filters directory for classes that inherit from PageFilterBase.
    """
    filters_dir = PathUtils.get_crawler_package_root() / "page_filters"
    discovered_filters: Dict[str, Type[PageFilterBase]] = {}
    base_module_path = "pydpiper_shell.core.page_filters"

    logger.debug("Scanning for page filters in:\n'%s'", filters_dir)

    for file_path in filters_dir.glob("*_filter.py"):
        try:
            module_name = f"{base_module_path}.{file_path.stem}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if not spec or not spec.loader:
                continue

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, PageFilterBase) and obj is not PageFilterBase:
                    filter_name = file_path.stem.replace("_filter", "")
                    discovered_filters[filter_name] = obj
                    logger.debug("Discovered page filter '%s'", filter_name)

        except Exception as e:
            logger.error("Failed to load filter module %s: %s", file_path.name, e, exc_info=True)

    return discovered_filters


def discover_handlers() -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, str]]:
    """
    Scans multiple handler directories, loads modules, and returns three dictionaries:
    1. A map of command names to their handler function.
    2. A map of command names to their hierarchy definition.
    3. A map of command names to their help text string.
    """
    discovered_handlers: Dict[str, Any] = {}
    discovered_hierarchies: Dict[str, Any] = {}
    discovered_help_texts: Dict[str, str] = {}

    handler_locations = [
        (PathUtils.get_shell_package_root() / "core" / "handlers", "pydpiper_shell.core.handlers"),
    ]

    for handlers_dir, base_module_path in handler_locations:
        logger.debug("Scanning for handlers in: '%s'", handlers_dir)

        if not handlers_dir.is_dir():
            logger.warning("Handlers directory not found, skipping: %s", handlers_dir)
            continue

        for file_path in sorted(handlers_dir.glob("**/*_handler.py")):
            try:
                relative_path = file_path.relative_to(handlers_dir)
                module_name_parts = list(relative_path.parts)
                module_name_parts[-1] = file_path.stem
                module_import_path = ".".join(module_name_parts)

                module_name = f"{base_module_path}.{module_import_path}"

                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if not spec or not spec.loader:
                    raise ImportError(f"Could not create spec for {file_path}")

                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                hierarchy = getattr(module, "COMMAND_HIERARCHY", None)

                for attr_name in dir(module):
                    if attr_name.startswith("handle_"):
                        handler_func = getattr(module, attr_name)
                        if callable(handler_func):
                            command_name = attr_name.replace("handle_", "")
                            discovered_handlers[command_name] = handler_func
                            if hierarchy is not None:
                                discovered_hierarchies[command_name] = hierarchy
                            logger.debug("Discovered command '%s'", command_name)

                    elif attr_name.endswith("_help_text"):
                        help_text_var = getattr(module, attr_name)
                        if isinstance(help_text_var, str):
                            command_name = attr_name.replace("_help_text", "")
                            discovered_help_texts[command_name] = help_text_var
                            logger.debug("Discovered help '%s'", command_name)

            except Exception as e:
                logger.error("Failed to load handler module %s: %s", file_path.name, e, exc_info=True)

    return discovered_handlers, discovered_hierarchies, discovered_help_texts