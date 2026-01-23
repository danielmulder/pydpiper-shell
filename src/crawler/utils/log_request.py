#File src/crawler/utils/log_request.py
import logging
from typing import Dict
from pydantic_core._pydantic_core import ValidationError
from crawler.model import Request

logger = logging.getLogger(__name__)

def log_request(self, url: str, fetch_result: Dict):
    try:
        request_log = Request(
            project_id=self.project_id,
            url=url,
            status_code=fetch_result.get("status", -1),
            headers=fetch_result.get("headers", {}),
            elapsed_time=fetch_result.get("elapsed_time", 0.0),
            timers=fetch_result.get("timers", {}),
            redirect_chain=fetch_result.get("redirect_chain", [])
        )
        self.requests_buffer.append(request_log)
    except ValidationError as e:
        logger.warning(
            "Pydantic validation failed while logging request for URL '%s': %s",
            url, e.errors()
        )
    except Exception as e:
        logger.error(
            "Unexpected error logging request for URL '%s': %s",
            url, e, exc_info=True
        )