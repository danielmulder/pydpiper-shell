import logging
import json
from typing import List, TypeVar, Any, Optional, Dict
from pydantic import BaseModel
import pandas as pd
from datetime import datetime
import sqlite3

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.utils.config_loader import get_nested_config
from pydpiper_shell.model import Project

from pydpiper_shell.core.managers.database_manager import DatabaseManager
from crawler.services.data_prepare_service import DataPrepareService

T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)

# --- Mapping from logical names to DB table names ---
TABLE_MAP = {
    "pages": "pages",
    "internal_links": "links",
    "external_links": "links",
    "requests": "requests",
    "project": "project",
    "page_elements": "page_elements",
    "audits": "audit_issues",
}


# --- End Mapping ---

class DatabaseAccessor:
    """
    Provides plugins with access to project-specific data via SQL queries,
    returning results primarily as Pandas DataFrames.
    """

    def __init__(self, project_id: int, ctx: ShellContext):
        self._project_id = project_id
        # --- FIX 2: Type hint update & DataPrepareService init ---
        self._db_mgr: DatabaseManager = ctx.db_mgr
        self._dps = DataPrepareService()

    def _load_as_dataframe(self, name: str) -> pd.DataFrame:
        """Helper to load table data into a Pandas DataFrame using SQL."""
        table_name = TABLE_MAP.get(name.lower())
        if not table_name:
            logger.error(f"Cannot load DataFrame: No table mapping found for '{name}'")
            return pd.DataFrame()

        logger.debug(f"Loading data from table '{table_name}' into DataFrame for project {self._project_id}...")
        try:
            conn = self._db_mgr.get_connection(self._project_id)
            sql_query = f"SELECT * FROM {table_name}"

            if name.lower() == "internal_links":
                sql_query += " WHERE is_external = 0"
            elif name.lower() == "external_links":
                sql_query += " WHERE is_external = 1"
            elif 'project_id' in self._get_table_columns(conn, table_name):
                if " WHERE " in sql_query:
                    sql_query += f" AND project_id = {self._project_id}"
                else:
                    sql_query += f" WHERE project_id = {self._project_id}"

            # logger.info(f"Executing SQL for DataFrame: {sql_query}")
            df = pd.read_sql_query(sql_query, conn)
            return df
        except Exception as e:
            logger.error(f"Unexpected error loading DataFrame from '{table_name}': {e}", exc_info=True)
            return pd.DataFrame()

    def _get_table_columns(self, conn: sqlite3.Connection, table_name: str) -> List[str]:
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in cursor.fetchall()]
        except sqlite3.Error:
            return []

    # --- LOAD METHODS ---
    def load_pages_df(self) -> pd.DataFrame:
        return self._load_as_dataframe("pages")

    def load_internal_links_df(self) -> pd.DataFrame:
        return self._load_as_dataframe("internal_links")

    def load_external_links_df(self) -> pd.DataFrame:
        return self._load_as_dataframe("external_links")

    def load_requests_df(self) -> pd.DataFrame:
        return self._load_as_dataframe("requests")

    def load_project(self) -> Optional[Project]:
        try:
            conn = self._db_mgr.get_connection(self._project_id)
            df = pd.read_sql_query(f"SELECT * FROM project WHERE id = ?", conn, params=(self._project_id,))
            if df.empty: return None
            project_data = df.iloc[0].to_dict()
            if 'created_at' in project_data and isinstance(project_data['created_at'], str):
                try:
                    project_data['created_at'] = datetime.fromisoformat(project_data['created_at'])
                except ValueError:
                    pass
            return Project(**project_data)
        except Exception:
            return None

    def load_images_df(self) -> pd.DataFrame:
        try:
            conn = self._db_mgr.get_connection(self._project_id)
            q = "SELECT page_id, image_url, alt_text, width, height FROM images WHERE project_id = ?"
            df = pd.read_sql_query(q, conn, params=(self._project_id,))
            if df.empty: return pd.DataFrame(columns=["page_id", "image_url", "alt_text", "width", "height"])
            return df.drop_duplicates(subset=["page_id", "image_url", "alt_text", "width", "height"])
        except Exception:
            return pd.DataFrame(columns=["page_id", "image_url", "alt_text", "width", "height"])

    def load_page_elements_df(self, element_types: Optional[List[str]] = None,
                              page_ids: Optional[List[int]] = None) -> pd.DataFrame:
        try:
            conn = self._db_mgr.get_connection(self._project_id)
            sql_query = f"SELECT * FROM page_elements WHERE project_id = ?"
            params: List[Any] = [self._project_id]
            if page_ids:
                sql_query += f" AND page_id IN ({', '.join('?' * len(page_ids))})"
                params.extend(page_ids)
            if element_types:
                sql_query += f" AND element_type IN ({', '.join('?' * len(element_types))})"
                params.extend(element_types)
            df = pd.read_sql_query(sql_query, conn, params=params)

            def try_json_loads(x):
                if isinstance(x, str) and x.startswith(('[', '{')):
                    try:
                        return json.loads(x)
                    except json.JSONDecodeError:
                        return x
                return x

            if 'content' in df.columns: df['content'] = df['content'].apply(try_json_loads)
            return df
        except Exception:
            return pd.DataFrame()

    def save_dataframe(self, name: str, df: pd.DataFrame, if_exists: str = 'replace') -> None:
        if df.empty: return
        table_name = f"plugin_{name}"
        try:
            conn = self._db_mgr.get_connection(self._project_id)
            if 'project_id' not in df.columns: df['project_id'] = self._project_id
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            logger.info(f"Successfully saved DataFrame to table '{table_name}'.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame to table '{table_name}': {e}", exc_info=True)

    def save_audit_issues(self, issues: List[Any]) -> None:
        """
        Persists list of AuditIssue objects.
        Translates Objects -> SQL + Tuples via DataPrepareService.
        """
        if not issues: return

        try:
            # Gebruik de prepare service (net als in de crawler facade)
            sql_query, tuples = self._dps.prepare_audit_issues(issues)

            if sql_query and tuples:
                self._db_mgr.save_batch(self._project_id, sql_query, tuples)
                logger.debug(f"Saved {len(tuples)} audit issues via PluginFacade.")
        except Exception as e:
            logger.error(f"Failed to save audit issues in PluginFacade: {e}", exc_info=True)


class PluginFacade:
    """Facade passed to plugins."""

    def __init__(self, project_id: int, ctx: ShellContext):
        if project_id > 0:
            self.project_id = project_id
            self.cache = DatabaseAccessor(project_id, ctx)
        else:
            self.project_id = 0
            self.cache = None
        self.logger = logger
        self.ctx = ctx

    def get_config(self, key_path: str, default: Any = None) -> Any:
        return get_nested_config(key_path, default)