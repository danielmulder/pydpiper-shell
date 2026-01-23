# src/crawler/services/data_prepare_service.py
import json
import logging

from pydantic import BaseModel
from typing import List, Any, Tuple

logger = logging.getLogger(__name__)


class DataPrepareService:
    """
    Central service for preparing data types for insertion into database.
    """

    def prepare_audit_issues(self, batch: List[Any]) -> Tuple[str, List[tuple]]:
        sql = """
                INSERT INTO audit_issues (
                    project_id, page_id, url, category, element_type, 
                    issue_code, severity, message, details, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

        tuples = []
        for item in batch:
            d = item.model_dump() if isinstance(item, BaseModel) else item

            # --- FIX: Converteer dict naar JSON string ---
            details = d.get("details")
            if isinstance(details, (dict, list)):
                details = json.dumps(details)
            # ---------------------------------------------

            tuples.append((
                d.get("project_id"),
                d.get("page_id"),
                str(d.get("url")),
                d.get("category"),
                d.get("element_type"),
                d.get("issue_code"),
                d.get("severity"),
                d.get("message"),
                details,  # Gebruik de geconverteerde variabele
                d.get("created_at")
            ))
        return sql, tuples

    def prepare_page_elements(self, batch: List[Any]) -> Tuple[str, List[tuple]]:
        sql = "INSERT OR IGNORE INTO page_elements (project_id, page_id, element_type, content) VALUES (?, ?, ?, ?)"
        tuples = []
        for item in batch:
            # Handle direct tuples if passed, or Dict/Model
            if isinstance(item, tuple) and len(item) == 4:
                tuples.append(item)
            else:
                d = item.model_dump() if isinstance(item, BaseModel) else item
                tuples.append((
                    d.get("project_id"),
                    d.get("page_id"),
                    d.get("element_type"),
                    d.get("content")
                ))
        return sql, tuples

    def prepare_page_metrics(self, batch: List[Any]) -> Tuple[str, List[tuple]]:
        sql = """
                INSERT OR REPLACE INTO plugin_page_metrics 
                (project_id, page_id, url, title_length, h1_length, meta_desc_length, total_images, missing_alt_tags, missing_alt_ratio, internal_link_count, external_link_count, incoming_link_count, has_canonical, word_count, server_time, broken_img_ratio) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        tuples = []
        for item in batch:
            d = item.model_dump() if isinstance(item, BaseModel) else item
            tuples.append((
                d.get("project_id"),
                d.get("page_id"),
                str(d.get("url")),  # FIX: Convert HttpUrl to string
                d.get("title_length"),
                d.get("h1_length"),
                d.get("meta_desc_length"),
                d.get("total_images"),
                d.get("missing_alt_tags"),
                d.get("missing_alt_ratio"),
                d.get("internal_link_count"),
                d.get("external_link_count"),
                d.get("incoming_link_count"),
                d.get("has_canonical"),
                d.get("word_count"),
                d.get("server_time"),
                d.get("broken_img_ratio")
            ))
        return sql, tuples

    def prepare_pages(self, batch: List[Any]) -> Tuple[str, List[tuple]]:
        sql = """
            INSERT OR IGNORE INTO pages (url, status_code, content, crawled_at, ipr) VALUES (?, ?, ?, ?, ?)
        """
        tuples = []
        for item in batch:
            d = item.model_dump() if isinstance(item, BaseModel) else item
            tuples.append((
                str(d.get("url")),  # FIX: Convert HttpUrl to string
                d.get("status_code"),
                d.get("content"),
                d.get("crawled_at"),
                d.get("ipr", 0.0)
            ))
        return sql, tuples

    def prepare_links(self, batch: List[Any], is_external: bool) -> Tuple[str, List[tuple]]:
        sql = """
            INSERT OR IGNORE INTO links 
            (project_id, source_url, target_url, anchor, rel, is_external) 
            VALUES (?, ?, ?, ?, ?, ?)
        """

        external_flag = 1 if is_external else 0
        tuples = []
        for item in batch:
            d = item.model_dump() if isinstance(item, BaseModel) else item
            tuples.append((
                int(d.get("project_id")),
                str(d.get("source_url")),
                str(d.get("target_url")),
                # anchor_text from object but stored in column 'anchor'
                d.get("anchor_text") or d.get("anchor"),
                str(d.get("rel")) if d.get("rel") else None,
                external_flag
            ))
        return sql, tuples

    def prepare_requests(self, project_id: int, batch: List[Any]) -> Tuple[str, List[tuple]]:
        sql = """
            INSERT INTO requests 
            (project_id, url, status_code, headers, redirect_chain, elapsed_time, timers, created_at) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        tuples = []
        for item in batch:
            d = item.model_dump() if isinstance(item, BaseModel) else item

            headers = json.dumps(d.get("headers")) if isinstance(d.get("headers"), (dict, list)) else d.get("headers")
            timers = json.dumps(d.get("timers")) if isinstance(d.get("timers"), (dict, list)) else d.get("timers")
            # Redirect chain is vaak een list, dus json dumpen of str()
            redirects = str(d.get("redirect_chain", []))

            tuples.append((
                int(project_id),
                str(d.get("url")),
                d.get("status_code"),
                headers,
                redirects,  # Toegevoegd
                d.get("elapsed_time", 0.0),
                timers,
                d.get("created_at") or d.get("timestamp")
            ))
        return sql, tuples