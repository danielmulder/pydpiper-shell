import logging
import json
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from typing import List, Dict, Any, Tuple, Set, Optional

import pandas as pd

from auditor.dom.builder import DOMBuilder
from auditor.dom.qngine import QNGINE
from auditor.model import AuditIssue
from auditor.managers.audit_ignore_manager import AuditIgnoreManager
from pydpiper_shell.core.managers.report_manager import ReportManager
from pydpiper_shell.core.managers.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


def _worker_audit_page(
        page_data: Tuple[str, str, int],
        project_id: int,
        ignored_images: Set[str],
        ignored_links: Set[str],
        status_map: Dict[str, int]
) -> Optional[Dict[str, Any]]:
    """
    Worker function to audit a single page in a separate process.
    Also extracts N-grams (Content Analysis) for bulk storage.
    """
    url, html, page_id = page_data
    if not html:
        return None

    builder = DOMBuilder()
    engine = QNGINE()

    results = {
        "issues": [],
        "export_rows": [],
        "stats": Counter(),
        "page_type_data": None,
        "ngrams_data": []  # BUFFER: Collecting n-grams here
    }

    try:
        # Parse the document (DOMBuilder handles N-gram calculation)
        sem_doc = builder.parse_doc(url, html, status_map=status_map)

        # --- 1. EXTRACT N-GRAMS FOR BUFFER ---
        # Data format for DB: (project_id, page_id, type, content_json)
        if sem_doc.body_unigrams:
            results['ngrams_data'].append(
                (project_id, page_id, 'meta_unigrams', json.dumps(sem_doc.body_unigrams))
            )
        if sem_doc.body_bigrams:
            results['ngrams_data'].append(
                (project_id, page_id, 'meta_bigrams', json.dumps(sem_doc.body_bigrams))
            )
        if sem_doc.body_trigrams:
            results['ngrams_data'].append(
                (project_id, page_id, 'meta_trigrams', json.dumps(sem_doc.body_trigrams))
            )

        # --- 2. RUN AUDIT RULES ---
        findings = engine.run_audit(sem_doc)

        # --- 3. PAGE TYPE DETECTION (Structured Data Analysis) ---
        page_type = "unknown"
        if sem_doc.structured_data:
            sd_str = json.dumps(sem_doc.structured_data).lower()
            if '"@type": "product"' in sd_str or '"@type": "individualproduct"' in sd_str:
                page_type = "product"
            elif '"@type": "collectionpage"' in sd_str or '"@type": "offerallocator"' in sd_str:
                page_type = "category"
            elif '"@type": "blogposting"' in sd_str or '"@type": "article"' in sd_str:
                page_type = "article"

        results['page_type_data'] = (project_id, page_id, 'page_type', page_type)

        for f in findings:
            # Apply exclusion/ignore logic
            if f['el'] == 'image' and ignored_images:
                if any(ign_img in f['msg'] for ign_img in ignored_images):
                    continue

            if f['el'] == 'anchor' and ignored_links:
                if any(ign_link in f['msg'] for ign_link in ignored_links):
                    continue

            results['stats'][(f['cat'], f['code'])] += 1

            results['issues'].append({
                "project_id": project_id,
                "page_id": page_id,
                "url": url,
                "category": f['cat'],
                "element_type": f['el'],
                "issue_code": f['code'],
                "severity": f['sev'],
                "message": f['msg']
            })

            results['export_rows'].append({
                "URL": url,
                "Category": f['cat'],
                "Type": f['el'],
                "Code": f['code'],
                "Severity": f['sev'],
                "Message": f['msg']
            })

        return results

    except Exception as e:
        logger.error(f"Worker failed on {url}: {e}")
        return {"error": str(e), "url": url}


class AuditController:
    """
    Orchestrates the auditing process, managing parallel execution,
    aggregation of results, and bulk database persistence.
    """

    def __init__(self, project_id: int, ignore_manager: AuditIgnoreManager):
        self.project_id = project_id
        self.ignore_manager = ignore_manager

        self.report_manager = ReportManager()
        self.db_manager = DatabaseManager()

        # Results Buffers
        self.audit_objects: List[AuditIssue] = []
        self.export_rows: List[Dict[str, Any]] = []
        self.stats = defaultdict(Counter)

        # Bulk Insert Buffers
        self.page_types_buffer: List[tuple] = []
        self.ngrams_buffer: List[tuple] = []

    def run_audit(
            self,
            df: pd.DataFrame,
            status_map: Optional[Dict[str, int]] = None,
            workers: int = 4,
            progress_callback=None
    ) -> Dict[str, Any]:
        """Runs the audit on a DataFrame and persists results in bulk."""
        total_rows = len(df)
        pages_with_issues = 0
        total_issues = 0

        # Reset Buffers
        self.audit_objects = []
        self.export_rows = []
        self.stats = defaultdict(Counter)
        self.page_types_buffer = []
        self.ngrams_buffer = []

        # Prepare task tuples
        tasks = []
        for row in df.itertuples():
            p_id = getattr(row, "id", getattr(row, "page_id", 0))
            tasks.append((row.url, getattr(row, 'content', ""), p_id))

        ign_img = self.ignore_manager.ignored_images
        ign_lnk = self.ignore_manager.ignored_links

        # Start parallel processing
        with ProcessPoolExecutor(max_workers=workers) as executor:
            func = partial(
                _worker_audit_page,
                project_id=self.project_id,
                ignored_images=ign_img,
                ignored_links=ign_lnk,
                status_map=status_map or {}
            )

            results_iter = executor.map(func, tasks)

            for i, result in enumerate(results_iter):
                if progress_callback:
                    progress_callback(i + 1, total_rows)

                if not result or "error" in result:
                    continue

                # Aggregate N-grams and Page Types
                if result.get('ngrams_data'):
                    self.ngrams_buffer.extend(result['ngrams_data'])
                if result.get('page_type_data'):
                    self.page_types_buffer.append(result['page_type_data'])

                # Aggregate Audit Issues
                if result['issues']:
                    pages_with_issues += 1
                    total_issues += len(result['issues'])
                    for key, count in result['stats'].items():
                        cat, code = key
                        self.stats[cat][code] += count
                    self.export_rows.extend(result['export_rows'])
                    for issue_dict in result['issues']:
                        self.audit_objects.append(AuditIssue(**issue_dict))

        # --- PERSISTENCE (Bulk Operations) ---
        self._save_metadata_to_db()
        self._generate_summary_report(total_rows, pages_with_issues, total_issues)

        return {
            "total_pages": total_rows,
            "pages_with_issues": pages_with_issues,
            "total_issues": total_issues,
            "stats": self.stats
        }

    def _save_metadata_to_db(self):
        """Internal helper to handle bulk SQL inserts."""
        sql = "INSERT OR REPLACE INTO page_elements (project_id, page_id, element_type, content) VALUES (?, ?, ?, ?)"

        if self.ngrams_buffer:
            logger.info(f"Bulk saving {len(self.ngrams_buffer)} N-gram records...")
            self.db_manager.save_batch(self.project_id, sql, self.ngrams_buffer)

        if self.page_types_buffer:
            logger.info(f"Bulk saving {len(self.page_types_buffer)} page type records...")
            self.db_manager.save_batch(self.project_id, sql, self.page_types_buffer)

    def _generate_summary_report(self, total, pages_with_issues, total_issues):
        """Constructs and saves the final JSON audit report."""
        breakdown_list = [
            {"category": cat, "code": code, "count": count}
            for cat, codes in self.stats.items()
            for code, count in codes.items()
        ]

        report_payload = {
            "summary": {
                "pages_analyzed": total,
                "pages_with_issues": pages_with_issues,
                "total_issues": total_issues
            },
            "breakdown": breakdown_list
        }

        self.report_manager.save_report(
            project_id=self.project_id,
            lib="auditor",
            category="audit_summary",
            name="scan_result",
            data=report_payload
        )
        logger.info(f"✅ Audit summary report saved for project {self.project_id}")

    # --- Result Getters ---
    def get_results_for_db(self) -> List[AuditIssue]:
        return self.audit_objects

    def get_results_for_export(self) -> List[Dict[str, Any]]:
        return self.export_rows

    def get_page_types_for_db(self) -> List[tuple]:
        return self.page_types_buffer