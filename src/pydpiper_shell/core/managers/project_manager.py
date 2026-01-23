import logging
import shutil
from datetime import datetime
from typing import List, Optional

from pydpiper_shell.model import Project
from pydpiper_shell.core.utils.path_utils import PathUtils
from pydpiper_shell.core.managers.database_manager import DatabaseManager, thread_local_storage

logger = logging.getLogger(__name__)


class ProjectManager:
    """
    Manages the Project lifecycle: creation, loading, listing, and deletion.

    Acts as the high-level orchestrator that delegates low-level data operations
    to the DatabaseManager.
    """

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the ProjectManager.

        Args:
            db_manager (Optional[DatabaseManager]): Dependency injection for the DB manager.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.cache_dir = PathUtils.get_cache_root()
        self._ensure_global_project_index()

    def _ensure_global_project_index(self) -> None:
        """Ensures the root cache directory structure exists."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # --- CREATE & INIT ---

    def create_project(
            self, name: str, start_url: str, mode: str = "discovery"
    ) -> Optional[Project]:
        """
        Creates a new project record and initializes its database schema.
        """
        new_id = self._generate_next_id()
        if new_id is None:
            logger.error("Failed to generate a valid Project ID.")
            return None

        logger.info(f"Initializing DB schema for project {new_id} via DatabaseManager...")
        try:
            self.db_manager.init_schema(new_id)
        except Exception as e:
            logger.error(f"Failed to initialize schema for project {new_id}: {e}")
            return None

        now = datetime.now().isoformat()
        project = Project(
            id=new_id,
            name=name,
            start_url=start_url,
            run_mode=mode,
            created_at=now
        )

        if self.save_project_metadata(project):
            return project
        return None

    def _generate_next_id(self) -> int:
        """Determines the next available Project ID."""
        existing_ids = self._scan_existing_ids()
        if not existing_ids:
            return 1
        return max(existing_ids) + 1

    def _scan_existing_ids(self) -> List[int]:
        """Scans the cache directory for numeric project folders."""
        ids = []
        if not self.cache_dir.exists():
            return ids
        for item in self.cache_dir.iterdir():
            if item.is_dir() and item.name.isdigit():
                ids.append(int(item.name))
        return sorted(ids)

    # --- SAVE / LOAD ---

    def save_project_metadata(self, project: Project) -> bool:
        """Saves project metadata using INSERT OR REPLACE for idempotency."""
        sql = """
            INSERT OR REPLACE INTO project 
            (id, name, start_url, run_mode, sitemap_url, total_time, pages, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            project.id,
            project.name,
            str(project.start_url),
            project.run_mode,
            project.sitemap_url,
            project.total_time,
            project.pages,
            project.created_at
        )

        try:
            self.db_manager.execute_query(project.id, sql, params)
            return True
        except Exception as e:
            logger.error(f"Failed to save project metadata: {e}")
            return False

    def load_project(self, project_id: int) -> Optional[Project]:
        """Loads a project object from the database."""
        sql = "SELECT * FROM project WHERE id = ?"
        try:
            row = self.db_manager.fetch_one(project_id, sql, (project_id,))
            if not row:
                return None

            return Project(
                id=row[0],
                name=row[1],
                start_url=row[2],
                run_mode=row[3],
                sitemap_url=row[4],
                total_time=row[5] if row[5] else 0.0,
                pages=row[6] if row[6] else 0,
                created_at=row[7]
            )
        except Exception as e:
            logger.error(f"Error mapping project row: {e}")
            return None

    def get_project_by_id(self, project_id: int) -> Optional[Project]:
        """Alias for load_project."""
        return self.load_project(project_id)

    def get_all_projects(self) -> List[Project]:
        """Loads all valid projects found in the cache directory (Standard Name)."""
        projects = []
        ids = self._scan_existing_ids()
        for pid in ids:
            p = self.load_project(pid)
            if p:
                projects.append(p)
        return projects

    def load_all_projects(self) -> List[Project]:
        """Alias for get_all_projects for backward compatibility."""
        return self.get_all_projects()

    # --- DELETE ---

    def delete_project(self, project_id: int) -> bool:
        """Permanently deletes a project and closes connections."""
        self.db_manager.close_project_connections(project_id)
        path = PathUtils.get_project_dir(project_id, self.cache_dir)
        if path.exists():
            try:
                shutil.rmtree(path)
                logger.info(f"Deleted project directory: {path}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete project directory {path}: {e}")
                return False
        return False