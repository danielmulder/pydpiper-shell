import logging
import pandas as pd
from urllib.parse import urlparse, unquote
from collections import defaultdict
from typing import Dict, Any, List, Tuple
from datetime import datetime

from auditor.managers.audit_data_manager import AuditorDataManager
from pydpiper_shell.core.utils.path_utils import PathUtils
from auditor.managers.audit_ignore_manager import AuditIgnoreManager
from auditor.dom.registry import DOMRegistry

logger = logging.getLogger(__name__)


class ReportController:
    """
    Controller responsible for generating report data for the frontend and exports.
    Utilizes AuditorDataManager (Facade) to fetch data from the database.
    """

    def __init__(self, project_id: int, data_manager: AuditorDataManager):
        self.project_id = project_id
        self.data_manager = data_manager

        cache_path = PathUtils.get_cache_root()
        self.ignore_manager = AuditIgnoreManager(project_id, cache_path)

    # --- HELPERS ---

    def _get_filtered_issues(self) -> pd.DataFrame:
        """Retrieves issues via the manager and filters out hidden codes."""
        df = self.data_manager.load_audit_issues_df(self.project_id)

        hidden = self.ignore_manager.get_hidden_issues()
        if not df.empty and hidden:
            df = df[~df['issue_code'].isin(hidden)]
        return df

    def load_pages_df(self, project_id: int) -> pd.DataFrame:
        """Loads all project pages into a DataFrame."""
        return self.data_manager.load_pages_df(project_id)

    # --- SITE VIEW ---

    def generate_report_data(self) -> Dict[str, Any]:
        """
        Generates the hierarchical tree data structure for the site view report.
        Also calculates issue summaries per page.
        """
        pages_df = self.data_manager.load_pages_df(self.project_id)
        issues_df = self._get_filtered_issues()
        page_types_map = self.data_manager.load_page_types_map(self.project_id)

        if pages_df.empty:
            return {"tree_data": {}, "total_pages": 0, "total_issues": 0}

        # Calculate issue statistics per page
        issues_summary = defaultdict(lambda: {"critical": 0, "warning": 0, "info": 0, "total": 0})
        if not issues_df.empty:
            for row in issues_df.itertuples():
                issues_summary[row.page_id]["total"] += 1
                sev = row.severity
                if sev == 'CRITICAL':
                    issues_summary[row.page_id]["critical"] += 1
                elif sev == 'WARNING':
                    issues_summary[row.page_id]["warning"] += 1
                elif sev == 'INFO':
                    issues_summary[row.page_id]["info"] += 1

        # Build the hierarchical tree structure based on URL paths
        root_node = {'children': {}, 'page_data': None}
        for row in pages_df.itertuples():
            url = row.url
            parsed = urlparse(url)
            path = unquote(parsed.path.strip("/"))
            if parsed.query:
                path += f"?{parsed.query}"

            parts = [p for p in path.split("/") if p]

            current = root_node
            for part in parts:
                if part not in current['children']:
                    current['children'][part] = {'children': {}, 'page_data': None}
                current = current['children'][part]

            current['page_data'] = {
                "row": row,
                "stats": issues_summary[row.id],
                "type": page_types_map.get(row.id, "unknown")
            }

        jstree_root, _ = self._convert_to_jstree(root_node, text="Website Root", is_root=True)
        return {
            "project_id": self.project_id,
            "tree_data": jstree_root,
            "total_pages": len(pages_df),
            "total_issues": len(issues_df)
        }

    def _convert_to_jstree(self, node: Dict, text: str, is_root: bool = False) -> Tuple[Dict, Dict]:
        """Recursively converts internal tree node structure to jsTree format."""
        my_stats = {"critical": 0, "warning": 0, "info": 0, "pages": 0}

        if node['page_data']:
            stats = node['page_data']['stats']
            my_stats["pages"] += 1
            my_stats["critical"] += stats["critical"]
            my_stats["warning"] += stats["warning"]
            my_stats["info"] += stats["info"]

        jstree_children = []
        for name, child_node in sorted(node['children'].items()):
            child_jstree, child_stats = self._convert_to_jstree(child_node, text=name)
            jstree_children.append(child_jstree)

            my_stats["pages"] += child_stats["pages"]
            my_stats["critical"] += child_stats["critical"]
            my_stats["warning"] += child_stats["warning"]
            my_stats["info"] += child_stats["info"]

        # Determine icon based on page type detected during audit
        icon = "ri-folder-line"
        jstree_data = {}
        if node['page_data']:
            p_type = node['page_data']['type']
            jstree_data = {"page_id": node['page_data']['row'].id, "type": p_type}
            if not jstree_children:
                if p_type == "product":
                    icon = "ri-shopping-bag-3-line"
                elif p_type == "category":
                    icon = "ri-layout-grid-line"
                elif p_type == "article":
                    icon = "ri-article-line"
                else:
                    icon = "ri-file-text-line"

        # Construct display text with HTML badges for issue counts
        display_text = text
        badges = []
        if my_stats["pages"] > 0 and jstree_children:
            badges.append(
                f"<span style='color:#6c757d; font-size:0.85em; margin-left:5px;'>[{my_stats['pages']}]</span>"
            )
        if my_stats["critical"] > 0:
            badges.append(
                f"<span style='color:#dc3545; font-weight:bold; margin-left:5px;'>({my_stats['critical']})</span>"
            )
        if my_stats["warning"] > 0:
            badges.append(
                f"<span style='color:#ffc107; font-weight:bold; margin-left:5px;'>({my_stats['warning']})</span>"
            )
        if my_stats["info"] > 0:
            badges.append(
                f"<span style='color:#3498db; font-weight:bold; margin-left:5px;'>({my_stats['info']})</span>"
            )

        return {
            "text": display_text + "".join(badges),
            "icon": icon,
            "state": {"opened": is_root},
            "children": jstree_children,
            "data": jstree_data
        }, my_stats

    def get_page_details(self, page_id: int) -> Dict[str, Any]:
        """
        Retrieves detailed information for a specific page.
        Includes filtered issues and content analysis (n-grams).
        """
        page = self.data_manager.get_page_by_id(self.project_id, page_id)
        if page is None:
            return {"error": "Page not found"}

        hidden = self.ignore_manager.get_hidden_issues()
        issues_df = self.data_manager.get_issues_for_page(self.project_id, page_id)

        if hidden and not issues_df.empty:
            issues_df = issues_df[~issues_df['issue_code'].isin(hidden)]

        issues = issues_df.to_dict('records')
        ngrams = self.data_manager.get_page_ngrams(self.project_id, page_id)

        return {
            "id": int(page['id']),
            "url": page['url'],
            "status_code": int(page['status_code']),
            "issues": issues,
            "ngrams": ngrams
        }

    # --- ISSUE VIEW ---

    def generate_issue_tree_data(self) -> List[Dict]:
        """Generates the grouped issue tree for the issue overview panel."""
        df = self._get_filtered_issues()
        if df.empty:
            return []

        summary = df.groupby(['category', 'issue_code']).agg(
            severity=('severity', lambda x: 'CRITICAL' if 'CRITICAL' in list(x) else (
                'WARNING' if 'WARNING' in list(x) else 'INFO')),
            count=('issue_code', 'count')
        ).reset_index()

        tree_data = []
        for cat in summary['category'].unique():
            cat_rows = summary[summary['category'] == cat]
            total_issues = cat_rows['count'].sum()
            children = []
            for row in cat_rows.itertuples():
                icon = "ri-price-tag-3-line"
                if row.severity == 'CRITICAL':
                    icon = "ri-error-warning-fill text-danger"
                elif row.severity == 'WARNING':
                    icon = "ri-alert-fill text-warning"
                elif row.severity == 'INFO':
                    icon = "ri-information-fill text-info"

                children.append({
                    "text": f"{row.issue_code} <span style='color:#777; font-size:0.9em;'>({row.count})</span>",
                    "icon": icon,
                    "data": {
                        "type": "issue",
                        "category": cat,
                        "code": row.issue_code,
                        "count": int(row.count)
                    }
                })
            tree_data.append({
                "text": f"{cat} <span style='color:#777; font-weight:bold;'>[{total_issues}]</span>",
                "icon": "ri-folder-shield-2-line",
                "state": {"opened": False},
                "children": children,
                "data": {"type": "category"}
            })
        return tree_data

    def get_urls_for_issue(self, category: str, issue_code: str) -> List[Dict]:
        """Retrieves all URLs affected by a specific issue code."""
        return self.data_manager.get_issue_details_with_urls(self.project_id, category, issue_code)

    # --- CONFIG ---

    def get_config_data(self) -> Dict[str, Any]:
        """Retrieves configuration data including available and hidden issue codes."""
        all_possible = DOMRegistry.get_all_possible_codes()
        db_codes = self.data_manager.get_all_issue_codes(self.project_id)

        all_possible = sorted(list(set(all_possible + db_codes)))
        return {
            "all_codes": all_possible,
            "hidden_codes": list(self.ignore_manager.get_hidden_issues())
        }

    def save_config(self, hidden_codes: list):
        """Saves the list of hidden issue codes."""
        self.ignore_manager.set_hidden_issues(hidden_codes)

    def create_excel_export(self) -> str:
        """Generates an Excel export with Management Summary, Action List, and Inventory."""
        output_dir = PathUtils.get_user_documents_dir()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = output_dir / f"audit_report_project_{self.project_id}_{timestamp}.xlsx"

        df_issues = self._get_filtered_issues()
        df_pages = self.data_manager.load_pages_df(self.project_id)

        if not df_pages.empty:
            df_pages = df_pages[['id', 'url', 'status_code', 'crawled_at']].sort_values(by='url')

        if not df_issues.empty and not df_pages.empty:
            df_full = pd.merge(
                df_issues,
                df_pages[['id', 'url']],
                left_on='page_id',
                right_on='id',
                how='left'
            )

            df_full = df_full.rename(columns={
                'severity': 'Severity',
                'category': 'Category',
                'issue_code': 'Code',
                'message': 'Message',
                'url': 'URL',
                'element_type': 'Element'
            })

            cols = ['Severity', 'Category', 'Element', 'Code', 'Message', 'URL']
            valid_cols = [c for c in cols if c in df_full.columns]
            df_action = df_full[valid_cols]

            if 'Code' in df_action.columns:
                df_summary = df_action.groupby(['Severity', 'Category', 'Element', 'Code']).size().reset_index(
                    name='Count'
                )
                severity_order = {'CRITICAL': 1, 'WARNING': 2, 'INFO': 3}
                df_summary['SevRank'] = df_summary['Severity'].map(severity_order)
                df_summary = df_summary.sort_values(by=['SevRank', 'Count'], ascending=[True, False]).drop(
                    columns=['SevRank']
                )
            else:
                df_summary = pd.DataFrame()

            severity_order = {'CRITICAL': 1, 'WARNING': 2, 'INFO': 3}
            df_action['SevRank'] = df_action['Severity'].map(severity_order)
            df_action = df_action.sort_values(by=['SevRank', 'Category']).drop(columns=['SevRank'])

        else:
            df_summary = pd.DataFrame(columns=['Severity', 'Category', 'Code', 'Count'])
            df_action = pd.DataFrame(columns=['Severity', 'Category', 'Code', 'Message', 'URL'])

        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df_summary.to_excel(writer, sheet_name="Management Summary", index=False)
                df_action.to_excel(writer, sheet_name="Action List", index=False)
                df_pages.to_excel(writer, sheet_name="Page Inventory", index=False)

                # Auto-adjust column widths for better scannability
                for sheet in writer.sheets.values():
                    for col in sheet.columns:
                        max_len = 0
                        col_letter = col[0].column_letter
                        for cell in col:
                            try:
                                if len(str(cell.value)) > max_len:
                                    max_len = len(str(cell.value))
                            except Exception:
                                pass
                        sheet.column_dimensions[col_letter].width = min((max_len + 2), 100)

            return str(filename)
        except PermissionError:
            raise Exception("Excel file is currently open. Please close it and try again.")
        except Exception as e:
            logger.error(f"Error writing Excel export: {e}")
            raise e