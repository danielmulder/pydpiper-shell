import json
import logging
from typing import Any, Dict, Union, Optional
from pydpiper_shell.core.utils.path_utils import PathUtils

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Loads the application configuration from settings.json."""
    try:
        # Determine the expected path of the configuration file
        config_path = PathUtils.get_shell_package_root() / "settings.json"

        if not config_path.exists():
            logger.warning("Configuration file 'settings.json' not found at %s. Using empty config.", config_path)
            # ... (fallback logic handled by returning empty dict)
            return {}

        with open(config_path, "r") as f:
            return json.load(f)

    except Exception as e:
        logger.error("Failed to load settings.json: %s", e, exc_info=True)
        return {}


CONFIG = load_config()


# 👇 THE NEW HELPER FUNCTION
def get_nested_config(key_path: str, default: Optional[Any] = None) -> Any:
    """
    Safely retrieves a nested value from the global CONFIG dictionary.

    Uses a dot as a separator, e.g., 'crawler.default_max_pages'.

    Args:
        key_path (str): The dotted path to the configuration value.
        default (Any, optional): The default value to return if the key is not found.

    Returns:
        Any: The configuration value or the provided default.
    """
    keys = key_path.split('.')
    value = CONFIG

    for key in keys:
        if isinstance(value, dict):
            # Attempt to retrieve the value for the current key
            value = value.get(key)
        else:
            # If the current level is not a dictionary, the path is invalid
            return default

    # Return the final value if it was found, otherwise return the default
    return value if value is not None else default