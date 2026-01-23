# src/auditor/managers/audit_data_manager.py
import logging
import json
from typing import List, Dict, Optional, Any

import pandas as pd

from pydpiper_shell.core.managers.database_manager import DatabaseManager
from pydpiper_shell.core.services.dataframe_service import DataFrameService

logger = logging.getLogger(__name__)


class AuditorDataManager:
    """
    Read-Only Facade for the Auditor.

    Acts as a translation layer between the ReportController (which expects DataFrames/objects)
    and the 'dumb' DatabaseManager (which executes raw SQL).
    This class contains all SQL-specific logic required for generating reports.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.dfs = DataFrameService()

    # --- DATAFRAME LOADERS (Bulk) ---

    def load_pages_df(self, project_id: int) -> pd.DataFrame:
        """
        Loads all pages for a specific project into a DataFrame.
        Ensures the 'id' column is cast to an integer.
        """
        sql = "SELECT * FROM pages"
        df = self.dfs.fetch_dataframe(project_id, sql)
        if not df.empty and 'id' in df.columns:
            df['id'] = df['id'].astype(int)
        return df

    def load_audit_issues_df(self, project_id: int) -> pd.DataFrame:
        """Loads all discovered audit issues into a DataFrame."""
        return self.dfs.fetch_dataframe(project_id, "SELECT * FROM audit_issues")

    def load_page_types_map(self, project_id: int) -> Dict[int, str]:
        """
        Retrieves a mapping of page_id -> page_type from the page_elements table.

        Returns:
            Dict[int, str]: A dictionary like {1: 'product', 2: 'category'}.
        """
        sql = "SELECT page_id, content FROM page_elements WHERE element_type = 'page_type'"
        try:
            df = self.dfs.fetch_dataframe(project_id, sql)
            if df.empty:
                return {}
            return dict(zip(df['page_id'], df['content']))
        except Exception as e:
            logger.warning(f"Failed to load page types: {e}")
            return {}

    # --- SPECIFIC QUERIES (Detail) ---

    def get_page_by_id(self, project_id: int, page_id: int) -> Optional[pd.Series]:
        """Retrieves a single page by its ID."""
        sql = "SELECT * FROM pages WHERE id = ?"
        df = self.dfs.fetch_dataframe(project_id, sql, params=(page_id,))
        if df.empty:
            return None
        return df.iloc[0]

    def get_issues_for_page(self, project_id: int, page_id: int) -> pd.DataFrame:
        """Retrieves all audit issues associated with a specific page ID."""
        sql = "SELECT * FROM audit_issues WHERE page_id = ?"
        return self.dfs.fetch_dataframe(project_id, sql, params=(page_id,))

    def get_page_ngrams(self, project_id: int, page_id: int) -> Dict[str, Dict[str, int]]:
        """
        Retrieves and parses N-gram data (unigrams, bigrams, trigrams) from page_elements.

        Returns:
            Dict containing the three ngram categories as dictionaries.
        """
        sql = """
            SELECT element_type, content 
            FROM page_elements 
            WHERE page_id = ? 
            AND element_type IN ('meta_unigrams', 'meta_bigrams', 'meta_trigrams')
        """
        results = {
            "meta_unigrams": {},
            "meta_bigrams": {},
            "meta_trigrams": {}
        }

        try:
            df = self.dfs.fetch_dataframe(project_id, sql, params=(page_id,))
            if not df.empty:
                for row in df.itertuples():
                    try:
                        # Parse the JSON string stored in the content column
                        parsed_data = json.loads(row.content)
                        if row.element_type in results:
                            results[row.element_type] = parsed_data
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to decode JSON for {row.element_type} on page {page_id}")
        except Exception as e:
            logger.error(f"Error fetching N-grams for page {page_id}: {e}")

        return results

    def get_issue_details_with_urls(
            self, project_id: int, category: str, issue_code: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieves detailed issue information including the associated URL via a JOIN.

        Returns:
            List[Dict]: A list of dictionaries containing issue and page details.
        """
        sql = """
            SELECT p.id, p.url, p.status_code, i.message, i.element_type, i.created_at
            FROM audit_issues i 
            JOIN pages p ON i.page_id = p.id
            WHERE i.category = ? AND i.issue_code = ? 
            ORDER BY p.url
        """
        try:
            df = self.dfs.fetch_dataframe(project_id, sql, params=(category, issue_code))
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching issue details: {e}")
            return []

    def get_all_issue_codes(self, project_id: int) -> List[str]:
        """Retrieves a list of all unique issue codes currently in the database."""
        sql = "SELECT DISTINCT issue_code FROM audit_issues"
        try:
            df = self.dfs.fetch_dataframe(project_id, sql)
            if not df.empty:
                return df['issue_code'].tolist()
            return []
        except Exception:
            return []

    # --- LEGACY / OTHER ---

    def get_connection(self, project_id: int):
        """Pass-through method to access the raw database connection for edge cases."""
        return self.db.get_connection(project_id)