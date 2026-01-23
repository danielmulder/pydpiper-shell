# src/crawler/model.py (Crawl Layer)
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from pandas._libs import json
from pydantic import BaseModel, HttpUrl, Field, field_validator

logger = logging.getLogger(__name__)


class Page(BaseModel):
    id: Optional[int] = None
    url: HttpUrl
    status_code: Optional[int] = None
    # HIER IS DE WIJZIGING: Content Type opslaan voor de Auditor
    content_type: Optional[str] = None
    crawled_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    content: Optional[str] = None

class Request(BaseModel):
    id: Optional[int] = None
    project_id: int
    url: HttpUrl
    status_code: int
    method: str
    headers: Dict[str, Any]
    elapsed_time: float
    timers: Dict[str, Any]
    redirect_chain: List[Dict[str, Any]]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PageMetric(BaseModel):
    page_id: int
    project_id: int
    url: str
    title_length: int
    h1_length: int
    meta_desc_length: int
    total_images: int
    missing_alt_tags: int
    missing_alt_ratio: float
    internal_link_count: int
    external_link_count: int
    incoming_link_count: int
    has_canonical: int
    word_count: int
    server_time: Optional[float] = None
    broken_img_ratio: Optional[float] = None

class PageElementData(BaseModel):
    project_id: int
    page_id: int
    element_type: str
    content: Union[str, Dict, List, None]

    @field_validator('content', mode='before')
    @classmethod
    def serialize_content(cls, v):
        if isinstance(v, (dict, list)):
            try:
                return json.dumps(v, ensure_ascii=False)
            except (TypeError, OverflowError) as json_err:
                     logger.warning(f"Could not JSON-encode content: {v!r}. Storing repr. Error: {json_err}")
                     return repr(v)
        elif v is None:
             return None
        return str(v)

class Link(BaseModel):
    id: Optional[int] = None
    project_id: int
    source_url: HttpUrl
    target_url: HttpUrl
    anchor: str
    rel: str

class Image(BaseModel):
    id: Optional[int] = None
    project_id: int
    page_id: int
    image_url: str
    alt_text: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None

    @field_validator("image_url", mode="before")
    @classmethod
    def _normalize_url(cls, v: Any) -> str:
        if v is None:
            raise ValueError("image_url is required")
        s = str(v).strip()
        if not s:
            raise ValueError("image_url cannot be empty")
        return s

    @field_validator("alt_text", mode="before")
    @classmethod
    def _normalize_alt(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        return s or None

    @staticmethod
    def _parse_dim(val: Any) -> Optional[int]:
        if val is None:
            return None
        s = str(val).strip()
        if not s:
            return None
        digits = "".join(ch for ch in s if ch.isdigit())
        return int(digits) if digits else None

    @field_validator("width", "height", mode="before")
    @classmethod
    def _normalize_dims(cls, v: Any) -> Optional[int]:
        return cls._parse_dim(v)

class CrawlSettings(BaseModel):
    max_pages: Optional[int] = Field(default=None)
    concurrency: int = Field(default=25)
    timeout: int = Field(default=20)
    flush_interval: int = Field(default=20)
    sanitize: bool = Field(default=False, description="Strip BOM and whitespace from HTML before saving.")