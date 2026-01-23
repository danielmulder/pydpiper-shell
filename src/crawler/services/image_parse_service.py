# file: src/parser/services/image_parse_service.py
from __future__ import annotations

from html.parser import HTMLParser
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from crawler.model import Image  # Pydantic Image


def _parse_int(val: Optional[str]) -> Optional[int]:
    if not val:
        return None
    s = "".join(ch for ch in val if ch.isdigit())
    return int(s) if s else None


def _pick_from_srcset(srcset: str) -> Optional[str]:
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        url = part.split()[0]
        if url:
            return url
    return None


class _ImgParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: List[tuple[str, Optional[str], Optional[int], Optional[int]]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "img":
            return
        a = {k.lower(): (v or "") for k, v in attrs}
        src = a.get("src") or a.get("data-src") or ""
        if not src and a.get("srcset"):
            src = _pick_from_srcset(a["srcset"] or "") or ""
        if not src:
            return
        alt = a.get("alt") or None
        w = _parse_int(a.get("width"))
        h = _parse_int(a.get("height"))
        self.items.append((src, alt, w, h))


class ImageParseService:
    """Parset <img> en levert Pydantic Image terug (geen opslag)."""

    def parse(self, *, html: str, base_url: str, project_id: int, page_id: int) -> List[Image]:
        p = _ImgParser()
        p.feed(html or "")
        out: List[Image] = []
        for src, alt, w, h in p.items:
            out.append(
                Image(
                    id=None,
                    project_id=project_id,
                    page_id=page_id,
                    image_url=urljoin(base_url, src),
                    alt_text=(alt.strip() if isinstance(alt, str) else None) or None,
                    width=w,
                    height=h,
                )
            )
        return out
