# src/pydpiper_shell/core/services/dataframe_service.py
import logging
from typing import Tuple, Any

import pandas as pd

from pydpiper_shell.core.managers.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class DataFrameService:
    """
    Central service for handling Pandas DataFrame operations.

    This service acts as an abstraction layer between the raw DatabaseManager
    and components that require data analysis capabilities (like the Auditor).
    It ensures the DatabaseManager remains decoupled from the Pandas library.
    """

    def __init__(self):
        """Initialize the service with a DatabaseManager instance."""
        self.db = DatabaseManager()

    def fetch_dataframe(
        self, project_id: int, query: str, params: Tuple[Any, ...] = ()
    ) -> pd.DataFrame:
        """
        Executes a SQL query and returns the results as a Pandas DataFrame.

        Args:
            project_id (int): The ID of the project to query.
            query (str): The raw SQL query to execute.
            params (tuple): Optional parameters for the SQL query to prevent injection.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
                          Returns an empty DataFrame if the query fails, ensuring safety.
        """
        conn = self.db.get_connection(project_id)
        try:
            return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            # Log the specific error but return an empty DF to prevent application crash
            logger.error(f"DataFrame fetch failed for project {project_id}: {e}")
            return pd.DataFrame()