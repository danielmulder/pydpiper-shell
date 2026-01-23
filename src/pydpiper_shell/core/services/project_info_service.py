# src/pydpiper_shell/core/services/project_info_service.py
import logging
from typing import Dict, Any, List, Optional
import pandas as pd # Still needed if we want to return DataFrames later, but not for basic stats

# --- UPDATED IMPORTS ---
from pydpiper_shell.core.managers.database_manager import DatabaseManager # Use the DB manager
# CacheManager and Page model are no longer needed here
# ---------------------

logger = logging.getLogger(__name__)


class ProjectInfoService:
    """
    A service to calculate and retrieve statistics from the project's SQLite database.
    """

    def __init__(self, project_id: int, db_mgr: DatabaseManager): # Updated type hint
        """
        Initializes the service for a specific project.

        Args:
            project_id: The ID of the project to analyze.
            db_mgr: An instance of the DatabaseManager to query data. # Updated docstring
        """
        self.project_id = project_id
        self.db_mgr = db_mgr
        # Internal cache for loaded pages is no longer needed

    # _load_pages method is removed as we query directly

    def get_total_pages(self) -> int:
        """Calculates the total number of pages crawled for the project using SQL."""
        try:
            conn = self.db_mgr.get_connection(self.project_id)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM pages")
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting total pages for project {self.project_id}: {e}", exc_info=True)
            return 0

    def get_avg_page_size(self, unit: str = 'KB') -> float:
        """
        Calculates the average size of the HTML content of the crawled pages using SQL.

        Args:
            unit (str): The unit for the output ('B', 'KB', 'MB').

        Returns:
            float: The average page size, rounded to two decimal places.
        """
        try:
            conn = self.db_mgr.get_connection(self.project_id)
            cursor = conn.cursor()
            # Calculate average length of non-null content directly in SQL
            cursor.execute("SELECT AVG(LENGTH(content)) FROM pages WHERE content IS NOT NULL")
            result = cursor.fetchone()
            avg_size_bytes = result[0] if result and result[0] is not None else 0.0

            if avg_size_bytes == 0.0:
                return 0.0

            unit_upper = unit.upper()
            if unit_upper == 'KB':
                return round(avg_size_bytes / 1024, 2)
            if unit_upper == 'MB':
                return round(avg_size_bytes / (1024 * 1024), 2)

            # Default to Bytes
            return round(avg_size_bytes, 2)
        except Exception as e:
            logger.error(f"Error getting avg page size for project {self.project_id}: {e}", exc_info=True)
            return 0.0

    def get_all_stats(self) -> Dict[str, Any]:
        """Retrieves all available statistics for the project."""
        # This now directly calls the SQL-backed methods
        return {
            "total_pages": self.get_total_pages(),
            "avg_page_size_kb": self.get_avg_page_size('KB')
            # Add more stats here as needed by querying the DB
        }

    # --- NEW METHOD (Example): Load data as DataFrame ---
    def get_pages_dataframe(self) -> pd.DataFrame:
        """Loads the pages table into a pandas DataFrame."""
        try:
            conn = self.db_mgr.get_connection(self.project_id)
            # Use pandas read_sql_query for easy DataFrame creation
            df = pd.read_sql_query("SELECT * FROM pages", conn)
            return df
        except Exception as e:
            logger.error(f"Error loading pages DataFrame for project {self.project_id}: {e}", exc_info=True)
            return pd.DataFrame() # Return empty DataFrame on error