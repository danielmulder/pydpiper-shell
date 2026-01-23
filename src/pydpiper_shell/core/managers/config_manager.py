# src/pydpiper_shell/core/managers/config_manager.py
import json
import logging
from typing import Any, Dict, Optional

from pydpiper_shell.core.utils.path_utils import PathUtils

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    A singleton class to manage the application's configuration.
    It loads settings from a file and allows for in-memory modifications.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Loads the configuration from the file."""
        self._config: Dict[str, Any] = {}
        self.reset()
        logger.debug("ConfigManager initialized.")

    def get_all(self) -> Dict[str, Any]:
        """Returns the entire current configuration dictionary."""
        return self._config

    def get_nested(self, key_path: str, default: Optional[Any] = None) -> Any:
        """
        Safely retrieves a nested value from the configuration.
        e.g., 'crawler.default_max_pages'.
        """
        keys = key_path.split('.')
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default
        return value if value is not None else default

    def set_nested(self, key_path: str, value: Any) -> bool:
        """
        Sets a nested value in the in-memory configuration.
        e.g., 'debug.level', 'INFO'
        """
        keys = key_path.split('.')
        d = self._config
        # Navigate to the second-to-last dictionary
        for key in keys[:-1]:
            d = d.setdefault(key, {})
            if not isinstance(d, dict):
                logger.error("Cannot set value: '%s' is not a dictionary.", key)
                return False

        # Get the original value to determine the type
        original_value = d.get(keys[-1])
        if original_value is not None:
            try:
                # Attempt to cast the new value to the type of the old one
                value = type(original_value)(value)
            except (ValueError, TypeError):
                logger.warning(
                    "Could not cast new value for '%s' to type %s. Storing as string.",
                    key_path, type(original_value).__name__
                )

        d[keys[-1]] = value
        logger.info("Configuration updated: %s = %s", key_path, value)
        return True

    def reset(self):
        """Resets the in-memory configuration from the settings.json file."""
        try:
            config_path = PathUtils.get_shell_package_root() / "settings.json"
            if not config_path.exists():
                logger.warning("settings.json not found at %s. Using empty config.", config_path)
                self._config = {}
                return
            with open(config_path, "r", encoding="utf-8") as f:
                self._config = json.load(f)
            logger.info("Configuration has been (re)loaded from settings.json.")
        except Exception as e:
            logger.error("Failed to load settings.json: %s", e, exc_info=True)
            self._config = {}


# The global singleton instance that the entire application will use.
config_manager = ConfigManager()