# src/pydpiper_shell/core/context/shell_context.py
import logging
from typing import Optional, Any, TYPE_CHECKING

from pydpiper_shell.core.managers.database_manager import DatabaseManager
from pydpiper_shell.core.managers.config_manager import config_manager
from pydpiper_shell.core.utils.path_utils import PathUtils

# Prevent circular imports during runtime, but retain type hinting for static analysis
if TYPE_CHECKING:
    from pydpiper_shell.model import Project

logger = logging.getLogger(__name__)


class ShellContext:
    """
    Manages session variables and the core state of the shell.
    Utilizes DatabaseManager for data persistence and holds the active project reference.
    """

    def __init__(self):
        self._vars = {}
        cache_root_dir = PathUtils.get_cache_root()

        # Initialize the DatabaseManager (consistent naming with app.py)
        self.db_manager = DatabaseManager()

        # Alias for backward compatibility if needed
        self.db_mgr = self.db_manager

        self.strict_mode = config_manager.get_nested("strict_mode.strict", True)
        self.project_manager = None  # Initialized later in app.py
        self.next_prompt_buffer: Optional[str] = None
        self.search_result_cache = None
        self.prompt_session: Optional[Any] = None

        # --- NEW: Active Project State ---
        self.current_project: Optional['Project'] = None

    def set_project(self, project: Optional['Project']) -> None:
        """
        Sets the currently active project and updates context variables.

        If a project is selected, its attributes are exported to the shell context.
        If deselected (None), project-specific variables are cleared.
        """
        self.current_project = project
        if project:
            self.export_project_variables(project)
        else:
            # Clear project-specific variables if project is deselected
            keys_to_remove = [k for k in self._vars.keys() if k.startswith("project.")]
            for k in keys_to_remove:
                del self._vars[k]

    def export_project_variables(self, project: 'Project') -> None:
        """
        Helper method to expose project attributes as shell variables.
        Example: @{project.id}, @{project.url}.
        """
        if not project:
            return
        self.set("project.id", str(project.id))
        self.set("project.name", project.name)
        self.set("project.url", str(project.start_url))
        self.set("project.mode", project.run_mode)

    def set(self, key: str, value: str) -> None:
        """Sets a context variable."""
        self._vars[key] = value

    def get(self, key: str) -> Optional[str]:
        """Retrieves a context variable. Returns None if key does not exist."""
        return self._vars.get(key)

    def __repr__(self) -> str:
        """Provides a string representation of the context state."""
        active_proj_id = self.current_project.id if self.current_project else "None"
        return f"<ShellContext active_project={active_proj_id} vars_count={len(self._vars)}>"