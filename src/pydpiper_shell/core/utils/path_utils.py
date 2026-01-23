# src/pydpiper_shell/core/utils/path_utils.py
from typing import Optional

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class PathUtils:
    """
    A central utility for reliably retrieving important project and user paths.
    """

    # --- Project specific paths

    @staticmethod
    def get_project_root() -> Path:
        """
        Returns the absolute path of the project root.
        Searches upwards for a directory containing 'src' and 'pyproject.toml'.
        """
        current_path = Path(__file__).resolve().parent
        while current_path != current_path.parent:
            src_dir = current_path / "src"
            pyproject_toml = current_path / "pyproject.toml"
            if src_dir.exists() and src_dir.is_dir() and pyproject_toml.exists() and pyproject_toml.is_file():
                return current_path
            current_path = current_path.parent
        raise FileNotFoundError(
            "Could not find the project root. Search for a directory containing 'src' and 'pyproject.toml'.")

    @staticmethod
    def get_content_root() -> Path:
        return PathUtils.get_project_root() / "src"

    @staticmethod
    def get_shell_package_root() -> Path:
        return PathUtils.get_content_root() / "pydpiper_shell"

    @staticmethod
    def get_crawler_package_root() -> Path:
        return PathUtils.get_content_root() / "crawler"

    @staticmethod
    def get_cache_root() -> Path:
        """
        Returns the root directory for the application cache located in the PROJECT ROOT.
        (e.g., /path/to/pydpiper/.pydpiper_cache)
        """
        return PathUtils.get_project_root() / ".pydpiper_cache"

    @staticmethod
    def get_plugins_dir() -> Path:
        return PathUtils.get_shell_package_root() / "core/plugins/modules"

    # --- User specific paths ---

    @staticmethod
    def get_user_config_dir() -> Path:
        """
        Returns the path to the user's .pydpiper config directory.
        (e.g., ~/.pydpiper/)
        """
        return Path.home() / ".pydpiper"

    @staticmethod
    def get_shell_history_file() -> Path:
        """
        Returns the path to the shell history file in the user's home directory.
        (e.g., ~/.pydpiper_shell_history)
        """
        return Path.home() / ".pydpiper_shell_history"

    @staticmethod
    def get_user_documents_dir() -> Path:
        """
        Returns the absolute path to the current user's Documents directory.
        """
        return Path.home() / "Documents"

    # --- Helper methods ---

    @staticmethod
    def get_project_dir(project_id: int, base_dir: Optional[Path] = None) -> Path:
        """
        Returns the directory for a specific project.
        Creates the directory if it doesn't exist.
        """
        root = base_dir if base_dir else PathUtils.get_cache_root()
        path = root / str(project_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def get_project_db_path(project_id: int, base_dir: Optional[Path] = None) -> Path:
        """Returns the path to the SQLite database file for a project."""
        return PathUtils.get_project_dir(project_id, base_dir) / "project_data.db"