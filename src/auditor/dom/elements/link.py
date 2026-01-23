from typing import Optional, List
from urllib.parse import urlparse
from pydantic import model_validator
from bs4 import Tag
from ..core import ElementBase, ElementDefinition, AuditResult, audit_spec


class LinkElement(ElementBase):
    """
    Data model for anchor (<a>) tags.
    Enriches base element with HTTP status codes and domain locality.
    """
    tag: str = "a"
    status_code: Optional[int] = None
    is_external: bool = False

    @property
    def href(self) -> Optional[str]:
        """Convenience property to access the href attribute."""
        return self.attrs.get('href')

    @model_validator(mode='after')
    def validate_link(self):
        """Perform initial data validation on the link attributes."""
        if self.href is None:
            return self

        href_stripped = self.href.strip()
        if not href_stripped:
            self.validation_errors.append("empty_href")

        if len(href_stripped) > 200:
            self.validation_errors.append("url_len")

        return self


def parse_link(tag: Tag, children: list) -> LinkElement:
    """Parses a <a> tag into the LinkElement model."""
    return LinkElement(
        tag="a",
        attrs=tag.attrs,
        text=tag.get_text(" ", strip=True)[:50],
        children=children
    )


# --- AUDIT RULES ---


@audit_spec(codes=["EMPTY_HREF", "URL_TOO_LONG"])
def check_link_integrity(node: LinkElement) -> List[AuditResult]:
    """Validates the basic integrity of the anchor tag (presence and length)."""
    res = []
    if "empty_href" in node.validation_errors:
        res.append(("EMPTY_HREF", "Link href is empty or whitespace", "anchor", "WARNING", "LINKS"))
    if "url_len" in node.validation_errors:
        res.append(("URL_TOO_LONG", f"URL exceeds 200 chars ({len(node.href)})", "anchor", "WARNING", "LINKS"))
    return res


@audit_spec(codes=["BROKEN_LINK", "HTTP_ERROR"])
def check_link_status(node: LinkElement) -> List[AuditResult]:
    """Checks the HTTP status code for both internal and external links."""
    res = []
    if node.status_code:
        if node.status_code == 404:
            res.append(("BROKEN_LINK", f"Link target not found (404): {node.href}", "anchor", "CRITICAL", "LINKS"))
        elif node.status_code >= 400:
            res.append((
                "HTTP_ERROR",
                f"Link target returned error ({node.status_code}): {node.href}",
                "anchor", "WARNING", "LINKS"
            ))
    return res


@audit_spec(codes=["URL_SPACE", "URL_UPPERCASE", "URL_UNDERSCORE", "URL_UNSAFE_CHAR", "URL_DOUBLE_SLASH"])
def check_seo_syntax(node: LinkElement) -> List[AuditResult]:
    """
    Checks SEO best practices for URL structure.
    Targeted specifically at INTERNAL links to ensure site structure health.
    """
    res = []

    # Ignore external sites (we don't control their SEO)
    if node.is_external:
        return res

    # Skip special protocols and anchors
    if not node.href or node.href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
        return res

    try:
        path = urlparse(node.href).path

        # 1. Spaces or encoded spaces
        if "%20" in path or " " in path:
            res.append(("URL_SPACE", f"URL contains spaces: {node.href}", "anchor", "WARNING", "SEO"))

        # 2. Uppercase characters (can cause duplicate content on case-sensitive servers)
        if any(c.isupper() for c in path):
            res.append(("URL_UPPERCASE", f"URL contains uppercase characters: {node.href}", "anchor", "INFO", "SEO"))

        # 3. Underscores (Google generally prefers hyphens over underscores)
        if "_" in path:
            res.append((
                "URL_UNDERSCORE",
                f"URL contains underscores (prefer hyphens): {node.href}",
                "anchor", "INFO", "SEO"
            ))

        # 4. Unsafe characters
        if any(c in "(),;~'+[]*" for c in path):
            res.append(("URL_UNSAFE_CHAR", f"URL contains unsafe characters: {node.href}", "anchor", "WARNING", "SEO"))

        # 5. Double slashes (often indicative of CMS or proxy rewrite errors)
        if "//" in path:
            res.append(("URL_DOUBLE_SLASH", f"URL contains double slashes: {node.href}", "anchor", "WARNING", "SEO"))

    except Exception:
        pass  # Ignore parse errors as integrity rules likely caught them

    return res


# --- ELEMENT DEFINITION ---

DEFINITION = ElementDefinition(
    tag_name="a",
    model=LinkElement,
    parser=parse_link,
    audit_rules=[check_link_integrity, check_link_status, check_seo_syntax]
)