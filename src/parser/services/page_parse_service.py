from __future__ import annotations

from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urljoin


class PageParseService:
    """
    A specialized extraction service for retrieving structural data from HTML.
    Note: This is a stateless service; orchestration and serialization are
    handled by the ParseController.
    """

    def __init__(self, page_content: str, base_url: str):
        if not page_content:
            raise ValueError("HTML content cannot be empty.")
        # We use lxml if available for a performance boost over html.parser
        self.soup = BeautifulSoup(page_content, "html.parser")
        self.base_url = base_url

    # -------- SEO & Meta Extraction --------

    def extract_page_title(self) -> str:
        """Retrieves the content of the <title> tag."""
        el = self.soup.find("title")
        return el.get_text(strip=True) if el else ""

    def extract_meta_description(self) -> str:
        """Retrieves the content of the <meta name='description'> tag."""
        meta = self.soup.find("meta", attrs={"name": "description"})
        return (meta.get("content") or "").strip() if meta else ""

    def extract_canonical_tag(self) -> str:
        """Retrieves the href of the <link rel='canonical'> tag."""
        link = self.soup.find("link", attrs={"rel": "canonical"})
        return (link.get("href") or "").strip() if link else ""

    def extract_headings(self) -> Dict[str, List[str]]:
        """Maps heading levels (h1-h6) to their respective text content."""
        out: Dict[str, List[str]] = {}
        for lvl in range(1, 7):
            tags = [t.get_text(" ", strip=True) for t in self.soup.find_all(f"h{lvl}")]
            if tags:
                out[f"h{lvl}"] = tags
        return out

    # -------- Social & Technical Metadata --------

    def extract_robots_meta(self) -> Dict[str, Any]:
        """Retrieves the robots directive from meta tags."""
        meta = self.soup.find("meta", attrs={"name": "robots"})
        if not meta:
            return {}
        content = (meta.get("content") or "").strip()
        return {"content": content} if content else {}

    def extract_open_graph_tags(self) -> Dict[str, str]:
        """Extracts all Open Graph (og:) properties for social sharing analysis."""
        og = {}
        for tag in self.soup.find_all("meta"):
            prop = tag.get("property") or ""
            if prop.startswith("og:"):
                og[prop] = tag.get("content") or ""
        return og

    def extract_structured_data(self) -> List[Dict[str, Any]]:
        """Extracts raw JSON-LD blocks for Schema.org validation."""
        data = []
        for script in self.soup.find_all("script", type="application/ld+json"):
            txt = script.string or script.get_text() or ""
            if not txt.strip():
                continue
            # We store as raw text; the Controller or Auditor handles JSON parsing
            data.append({"@raw": txt})
        return data

    # -------- Image Extraction Logic --------

    @staticmethod
    def _pick_from_srcset(srcset: str) -> Optional[str]:
        """Helper to pick the first candidate URL from a srcset string."""
        for part in srcset.split(","):
            part = part.strip()
            if not part:
                continue
            url = part.split()[0]
            if url:
                return url
        return None

    @staticmethod
    def _parse_int(val: Optional[str]) -> Optional[int]:
        """Cleans and converts dimension strings (e.g., '100px') to integers."""
        if not val:
            return None
        digits = "".join(ch for ch in val if ch.isdigit())
        return int(digits) if digits else None

    def extract_images(self) -> List[Dict[str, Any]]:
        """
        Extracts image metadata including URLs, alt text, and dimensions.
        Includes a filter for transparent 1x1 tracking pixels (data-uris).
        """
        out: List[Dict[str, Any]] = []
        for img in self.soup.find_all("img"):
            # Normalize attributes to lowercase
            attrs = {k.lower(): (v or "") for k, v in img.attrs.items()}

            # Fallback chain for image sources
            src = attrs.get("src") or attrs.get("data-src") or ""
            if not src and attrs.get("srcset"):
                src = self._pick_from_srcset(attrs.get("srcset") or "") or ""

            if not src:
                continue

            # Filter out common base64 placeholder/tracking GIFs
            if src.startswith("data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///"):
                continue

            alt = attrs.get("alt") or None
            width = self._parse_int(attrs.get("width"))
            height = self._parse_int(attrs.get("height"))

            out.append({
                "image_url": urljoin(self.base_url, src),
                "alt_text": (alt.strip() if isinstance(alt, str) else None) or None,
                "width": width,
                "height": height,
            })
        return out