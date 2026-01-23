from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from tqdm.auto import tqdm

from crawler.model import Page, PageElementData
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.utils.parallel_workers import parse_page_worker
from pydpiper_shell.core.managers.config_manager import config_manager

logger = logging.getLogger(__name__)


class ParseController:
    """
    Orchestrates the standalone extraction of DOM elements from crawled pages.
    Utilizes multiprocessing to maximize throughput for CPU-intensive HTML parsing.
    """

    def __init__(self, *, default_workers: Optional[int] = None) -> None:
        self.default_workers = default_workers or (os.cpu_count() or 4)

    def _load_pages(self, project_id: int, ctx: ShellContext, show_progress: bool) -> List[Page]:
        """Loads successful 200 OK pages with valid content from the database."""
        conn = ctx.db_mgr.get_connection(project_id)
        sql = (
            "SELECT id, url, content, status_code, crawled_at "
            "FROM pages WHERE content IS NOT NULL AND content != '' AND status_code = 200"
        )
        df = pd.read_sql_query(sql, conn)
        if df.empty:
            return []

        rows = df.to_dict("records")
        pages: List[Page] = []
        iterator = rows if not show_progress else tqdm(rows, desc="Preparing pages", unit="page", leave=False)

        for row in iterator:
            pages.append(Page(**row))
        return pages

    @staticmethod
    def _serialize_content(value: Any) -> Optional[str]:
        """Serializes complex content types (dict/list) into JSON strings for database storage."""
        if value in (None, "", [], {}):
            return None
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False)
            except (TypeError, OverflowError):
                return repr(value)
        return str(value)

    def _persist_elements(
            self,
            project_id: int,
            elements: Iterable[PageElementData],
            ctx: ShellContext,
            show_progress: bool,
    ) -> int:
        """Batch persists extracted elements (titles, meta, etc.) into the database."""
        tuples: List[Tuple[int, int, str, Optional[str]]] = []
        iterable = list(elements)

        if show_progress:
            iterable = list(tqdm(iterable, desc="Converting elements", unit="element", leave=False))

        for e in iterable:
            content_str = e.content if isinstance(e.content, (str, type(None))) else self._serialize_content(e.content)
            tuples.append((e.project_id, e.page_id, e.element_type, content_str))

        if not tuples:
            return 0

        sql_query = """
            INSERT OR IGNORE INTO page_elements 
            (project_id, page_id, element_type, content) 
            VALUES (?, ?, ?, ?)
        """

        if show_progress:
            with tqdm(total=len(tuples), desc="Saving elements", unit="row", leave=False) as bar:
                ctx.db_mgr.save_batch(project_id, sql_query, tuples)
                bar.update(len(tuples))
        else:
            ctx.db_mgr.save_batch(project_id, sql_query, tuples)
        return len(tuples)

    def _persist_images(
            self,
            project_id: int,
            images_tuples: List[Tuple[int, int, Optional[str], Optional[str], Optional[int], Optional[int]]],
            ctx: ShellContext,
            show_progress: bool,
    ) -> int:
        """Batch persists image metadata into the images table."""
        if not images_tuples:
            return 0

        sql_query = """
            INSERT INTO images 
            (project_id, page_id, image_url, alt_text, width, height) 
            VALUES (?, ?, ?, ?, ?, ?)
        """

        if show_progress:
            with tqdm(total=len(images_tuples), desc="Saving images", unit="img", leave=False) as bar:
                ctx.db_mgr.save_batch(project_id, sql_query, images_tuples)
                bar.update(len(images_tuples))
        else:
            ctx.db_mgr.save_batch(project_id, sql_query, images_tuples)
        return len(images_tuples)

    def _resolve_elements(
            self,
            *,
            elements: Optional[Iterable[str]],
            include_images: Optional[bool],
    ) -> List[str]:
        """Determines which elements to extract based on CLI arguments and config defaults."""
        allowed = {
            "page_title", "meta_description", "canonical_tag",
            "headings", "robots_meta", "open_graph_tags",
            "structured_data", "images",
        }

        # Check configuration for image parsing defaults
        parse_cfg = config_manager.get_nested("parser", {}) or {}
        parse_img_cfg = bool(parse_cfg.get("parse_img", False))

        if elements is None:
            # Standard fast set + conditional images
            elems = [
                "page_title", "meta_description", "canonical_tag",
                "headings", "robots_meta", "open_graph_tags", "structured_data",
            ]
            if include_images is True or (include_images is None and parse_img_cfg):
                elems.append("images")
            return elems

        # Normalize and filter user-provided element list
        elems_norm = [e.strip().lower() for e in elements if e and e.strip()]
        elems_norm = [e for e in elems_norm if e in allowed]

        # Respect explicit image overrides
        if include_images is True and "images" not in elems_norm:
            elems_norm.append("images")
        if include_images is False and "images" in elems_norm:
            elems_norm = [e for e in elems_norm if e != "images"]

        return elems_norm

    def parse_project(
            self,
            *,
            project_id: int,
            ctx: ShellContext,
            elements: Optional[Iterable[str]] = None,
            workers: Optional[int] = None,
            show_progress: bool = True,
            include_images: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Executes the parallel parsing routine for the specified project.
        Returns a dictionary containing execution statistics.
        """
        pages = self._load_pages(project_id, ctx, show_progress=show_progress)
        if not pages:
            return self._empty_stats(project_id)

        elements_norm = self._resolve_elements(elements=elements, include_images=include_images)
        n_workers = int(workers or self.default_workers)

        start = time.perf_counter()
        ok, ko = 0, 0
        collected: List[PageElementData] = []
        image_rows: List[Tuple[int, int, Optional[str], Optional[str], Optional[int], Optional[int]]] = []

        with ProcessPoolExecutor(max_workers=n_workers) as pool:
            futures = {
                pool.submit(parse_page_worker, p, elements_norm, project_id): p.id
                for p in pages
            }
            iterator = as_completed(futures)
            if show_progress:
                iterator = tqdm(iterator, total=len(futures), desc="Parsing & Preparing", unit=" page")

            for fut in iterator:
                pid = futures[fut]
                try:
                    result_json = fut.result()
                    if not result_json or not isinstance(result_json, str):
                        ko += 1
                        continue

                    extracted = json.loads(result_json)
                    if not isinstance(extracted, dict):
                        ko += 1
                        continue

                    ok += 1
                    for etype, raw in extracted.items():
                        if etype == "images":
                            for img in (raw or []):
                                w, h = self._parse_dimensions(img)
                                image_rows.append(
                                    (int(project_id), int(pid), img.get("image_url"), img.get("alt_text"), w, h))
                            continue

                        content_str = self._serialize_content(raw)
                        if content_str:
                            collected.append(PageElementData(project_id=project_id, page_id=pid, element_type=etype,
                                                             content=content_str))

                except Exception as e:
                    ko += 1
                    logger.error("Failed to process page %s: %s", pid, e, exc_info=True)

        saved_elements = self._persist_elements(project_id, collected, ctx, show_progress=show_progress)
        saved_images = self._persist_images(project_id, image_rows, ctx, show_progress=show_progress)
        dur = time.perf_counter() - start

        return {
            "project_id": project_id,
            "pages_total": len(pages),
            "pages_success": ok,
            "pages_failed": ko,
            "elements_saved": saved_elements,
            "images_saved": saved_images,
            "duration_s": round(dur, 3),
            "pages_per_s": round((len(pages) / dur) if dur > 0 else 0.0, 2),
        }

    def _parse_dimensions(self, img_dict: Dict) -> Tuple[Optional[int], Optional[int]]:
        """Helper to safely extract and convert image dimensions."""

        def to_int(val):
            if isinstance(val, int): return val
            if isinstance(val, str) and val.isdigit(): return int(val)
            return None

        return to_int(img_dict.get("width")), to_int(img_dict.get("height"))

    def _empty_stats(self, project_id: int) -> Dict[str, Any]:
        """Returns a default stats dictionary for empty projects."""
        return {
            "project_id": project_id, "pages_total": 0, "pages_success": 0,
            "pages_failed": 0, "elements_saved": 0, "images_saved": 0,
            "duration_s": 0.0, "pages_per_s": 0.0,
        }