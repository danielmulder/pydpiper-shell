# file: src/pydpiper_shell/core/utils/parallel_workers.py
import logging
from typing import Optional, List, Dict, Any
import json  # <— ADD

from crawler.model import Page
from parser.services.page_parse_service import PageParseService

logger = logging.getLogger(__name__)


def parse_page_worker(
    page: Page,
    elements_to_extract: Optional[List[str]],
    project_id: int,
) -> Optional[str]:  # <— CHANGE: return JSON str
    """
    Worker function parsing a Page object.
    Returns a JSON string of the extracted elements (or None on error).
    """
    if not page.content or page.id is None:
        logger.debug(f"Worker skip page {page.url} (ID: {page.id}): Missing content or ID.")
        return None

    try:
        svc = PageParseService(page.content, str(page.url))
        out: Dict[str, Any] = {}

        wanted = set(elements_to_extract) if elements_to_extract else {
            "page_title", "meta_description", "canonical_tag",
            "headings", "robots_meta", "open_graph_tags",
            "structured_data", "images"
        }

        if "page_title" in wanted:
            out["page_title"] = svc.extract_page_title()
        if "meta_description" in wanted:
            out["meta_description"] = svc.extract_meta_description()
        if "canonical_tag" in wanted:
            out["canonical_tag"] = svc.extract_canonical_tag()
        if "headings" in wanted:
            out["headings"] = svc.extract_headings()
        if "robots_meta" in wanted:
            out["robots_meta"] = svc.extract_robots_meta()
        if "open_graph_tags" in wanted:
            out["open_graph_tags"] = svc.extract_open_graph_tags()
        if "structured_data" in wanted:
            out["structured_data"] = svc.extract_structured_data()
        if "images" in wanted:
            out["images"] = svc.extract_images()

        # ⬇️ Serialize to JSON to avoid complex pickling on Windows spawn
        return json.dumps(out, ensure_ascii=False, default=str)

    except Exception as e:
        logger.error(f"WORKER ERROR parsing page {page.url} (ID: {page.id}): {e}", exc_info=True)
        return None
