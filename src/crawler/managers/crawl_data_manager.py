import logging
import json
from typing import List, Dict, Any, Union, Tuple

import pandas as pd
from pydantic import BaseModel
from pydpiper_shell.core.managers.database_manager import DatabaseManager
from crawler.services.data_prepare_service import DataPrepareService
from pydpiper_shell.core.services.dataframe_service import DataFrameService

logger = logging.getLogger(__name__)


class CrawlDataManager:
    """
    Smart Facade acting as an adapter between business logic and the dumb DatabaseManagerDev.
    """

    def __init__(self, delegate: DatabaseManager) -> None:
        self.delegate = delegate
        self.dps = DataPrepareService()
        self.dfs = DataFrameService()

    def get_connection(self, project_id):
        self.delegate.get_connection(project_id)

    def save(
            self,
            project_id: int,
            name: str,
            data: Union[BaseModel, List[BaseModel], List[Dict[str, Any]]]
    ) -> None:
        """
        Routes the save command to preparation logic and delegates execution.
        """
        if not data:
            return

        batch = data if isinstance(data, list) else [data]
        if not batch:
            return

        sql_query = ""
        tuples = []

        try:
            if name == "pages":
                # sql_query, tuples = self._prepare_pages(batch)
                sql_query, tuples = self.dps.prepare_pages(batch)

            # --- COMBINED LOGIC FOR LINKS (Single Table) ---
            elif name == "internal_links":
                sql_query, tuples = self.dps.prepare_links(batch, is_external=False)
                # print(f"batch: {batch}")

            elif name == "external_links":
                sql_query, tuples = self.dps.prepare_links(batch, is_external=True)
            # -----------------------------------------------

            elif name == "requests":
                sql_query, tuples = self.dps.prepare_requests(project_id, batch)

            elif name == "audit_issues":
                sql_query, tuples = self.dps.prepare_audit_issues(batch)

            elif name == "page_elements":
                sql_query, tuples = self.dps.prepare_page_elements(batch)

            elif name == "plugin_page_metrics":
                sql_query, tuples = self.dps.prepare_page_metrics(batch)

            else:
                logger.warning("No preparation logic defined for table '%s'", name)
                return

            if sql_query and tuples:
                self.delegate.save_batch(project_id, sql_query, tuples)

        except Exception as e:
            logger.error("Error preparing/saving batch for '%s': %s", name, e, exc_info=True)

    # --- DELEGATION HELPERS ---

    def init_db_schema(self, project_id: int):
        self.delegate.init_schema(project_id)

    def clear_crawl_data(self, project_id: int):
        # Update tables list to match the single 'links' table schema
        tables_to_clear = [
            "pages",
            "links",  # One table for both internal/external
            "requests",
            "page_elements",
            "plugin_page_metrics",
            "images",
            "audit_issues"
        ]
        self.delegate.clear_tables(project_id, tables_to_clear)

    def load_pages_df(self, project_id: int) -> pd.DataFrame:
        """Loads all pages into a DataFrame."""
        return self.dfs.fetch_dataframe(project_id, "SELECT * FROM pages")

    def load_requests_df(self, project_id: int) -> pd.DataFrame:
        """Loads all request logs into a DataFrame."""
        return self.dfs.fetch_dataframe(project_id, "SELECT * FROM requests")

    def load_internal_links_df(self, project_id: int) -> pd.DataFrame:
        """Loads internal links (is_external=0) from the single links table."""
        sql = "SELECT * FROM links WHERE is_external = 0"
        return self.dfs.fetch_dataframe(project_id, sql)

    def load_external_links_df(self, project_id: int) -> pd.DataFrame:
        """Loads external links (is_external=1) from the single links table."""
        sql = "SELECT * FROM links WHERE is_external = 1"
        return self.dfs.fetch_dataframe(project_id, sql)