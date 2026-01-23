from __future__ import annotations

from html.parser import HTMLParser
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from crawler.model import Image


def _parse_int(val: Optional[str]) -> Optional[int]:
    """Extracts the first sequence of digits from a string and converts to int."""
    if not val:
        return None
    # Handles cases like '100px' or '100%'
    s = "".join(ch for ch in val if ch.isdigit())
    return int(s) if s else None


def _pick_from_srcset(srcset: str) -> Optional[str]:
    """Extracts the first URL found in a srcset attribute."""
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        # srcset format is usually 'url size, url size'
        url = part.split()[0]
        if url:
            return url
    return None


class _ImgParser(HTMLParser):
    """
    High-speed, dependency-free SAX parser specifically for <img> tags.
    Streaming approach is significantly faster than DOM-based parsing.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: List[tuple[str, Optional[str], Optional[int], Optional[int]]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "img":
            return

        # Case-insensitive attribute mapping
        attr_dict = {k.lower(): (v or "") for k, v in attrs}

        # Priority: src -> data-src (lazy loading) -> srcset
        src = attr_dict.get("src") or attr_dict.get("data-src") or ""
        if not src and attr_dict.get("srcset"):
            src = _pick_from_srcset(attr_dict["srcset"] or "") or ""

        if not src:
            return

        alt = attr_dict.get("alt") or None
        w = _parse_int(attr_dict.get("width"))
        h = _parse_int(attr_dict.get("height"))

        self.items.append((src, alt, w, h))


class ImageParseService:
    """
    Service responsible for extracting image metadata from HTML content.
    Returns a list of Pydantic Image models.
    """

    def parse(self, *, html: str, base_url: str, project_id: int, page_id: int) -> List[Image]:
        """
        Parses the provided HTML and resolves relative image URLs using base_url.
        """
        parser = _ImgParser()
        parser.feed(html or "")

        images: List[Image] = []
        for src, alt, w, h in parser.items:
            # Resolve relative paths (e.g., /img.jpg) to absolute URLs
            absolute_url = urljoin(base_url, src)

            images.append(
                Image(
                    id=None,
                    project_id=project_id,
                    page_id=page_id,
                    image_url=absolute_url,
                    alt_text=(alt.strip() if isinstance(alt, str) else None) or None,
                    width=w,
                    height=h,
                )
            )
        return images