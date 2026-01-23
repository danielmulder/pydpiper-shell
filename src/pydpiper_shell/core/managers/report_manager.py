import json
import logging
from typing import Dict, Any, Optional, List

from pydpiper_shell.core.managers.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class ReportManager:
    """
    Manages storage/retrieval of reports and audit data.
    Uses DatabaseManagerDev for raw SQL access.
    """

    def __init__(self, dbm=None):
        self.dbm = DatabaseManager()

    def _tuple_to_dict(self, row: tuple) -> Dict[str, Any]:
        if not row: return None
        return {
            "id": row[0],
            "project_id": row[1],
            "lib": row[2],
            "category": row[3],
            "name": row[4],
            "data": row[5],
            "created_at": row[6]
        }

    # --- REPORT STORAGE ---
    def save_report(self, project_id: int, lib: str, category: str, name: str, data: Dict[str, Any]) -> int:
        sql = "INSERT INTO reports (project_id, lib, category, name, data) VALUES (?, ?, ?, ?, ?)"
        try:
            json_payload = json.dumps(data)
            return self.dbm.execute_query(project_id, sql, (project_id, lib, category, name, json_payload))
        except Exception as e:
            logger.error(f"Failed to save report '{name}': {e}")
            return -1

    def get_latest_report(self, project_id: int, lib: str = None, category: str = None, name: str = None) -> Optional[
        Dict[str, Any]]:
        sql = "SELECT * FROM reports WHERE project_id = ?"
        params = [project_id]
        if lib:
            sql += " AND lib = ?"
            params.append(lib)
        if category:
            sql += " AND category = ?"
            params.append(category)
        if name:
            sql += " AND name = ?"
            params.append(name)
        sql += " ORDER BY created_at DESC LIMIT 1"

        try:
            row = self.dbm.fetch_one(project_id, sql, tuple(params))
            if not row: return None

            if isinstance(row, tuple):
                row = self._tuple_to_dict(row)

            if row.get('data') and isinstance(row['data'], str):
                try:
                    row['data'] = json.loads(row['data'])
                except:
                    pass
            return row
        except Exception as e:
            logger.error(f"Failed to fetch report: {e}")
            return None

    # --- UPDATED METHODS FOR ISSUES TAB ---

    def get_issue_tree_structure(self, project_id: int) -> List[Dict[str, Any]]:
        """Bouwt de JSTree data structuur voor issues."""
        sql = """
            SELECT category, issue_code, COUNT(*) as count 
            FROM audit_issues 
            WHERE project_id = ? 
            GROUP BY category, issue_code
            ORDER BY category, issue_code
        """
        try:
            rows = self.dbm.fetch_all(project_id, sql, (project_id,))

            tree = []
            categories = {}

            for row in rows:
                cat, code, count = row[0], row[1], row[2]

                if cat not in categories:
                    cat_node = {
                        "text": cat,
                        "icon": "ri-folder-line",
                        "state": {"opened": False},
                        "children": []
                    }
                    categories[cat] = cat_node
                    tree.append(cat_node)

                # FIX: Data structuur aangepast voor jouw template
                issue_node = {
                    "text": f"{code} ({count})",
                    "icon": "ri-file-warning-line",
                    "data": {
                        "type": "issue",  # Nodig voor jouw JS check
                        "category": cat,  # 'category' ipv 'cat'
                        "code": code,
                        "count": count
                    }
                }
                categories[cat]["children"].append(issue_node)

            return tree
        except Exception as e:
            logger.error(f"Error building issue tree: {e}")
            return []

    def get_urls_for_issue(self, project_id: int, category: str, issue_code: str) -> List[Dict[str, Any]]:
        """
        Haalt rijke data op (URL + Status Code + Message) voor de drilldown tabel.
        Doet een JOIN met de pages tabel om de status code op te halen.
        """
        sql = """
            SELECT 
                ai.page_id, 
                ai.url, 
                ai.message,
                p.status_code
            FROM audit_issues ai
            LEFT JOIN pages p ON ai.page_id = p.id
            WHERE ai.project_id = ? AND ai.category = ? AND ai.issue_code = ?
        """
        try:
            rows = self.dbm.fetch_all(project_id, sql, (project_id, category, issue_code))

            # FIX: Return list of dicts ipv list of strings
            return [
                {
                    "id": r[0],
                    "url": r[1],
                    "message": r[2],
                    "status_code": r[3] if r[3] is not None else 0
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Error fetching issue URLs: {e}")
            return []