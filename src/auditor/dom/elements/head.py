from typing import List, Any
from bs4 import Tag
from ..core import ElementBase, ElementDefinition, AuditResult, audit_spec


class HeadElement(ElementBase):
    """
    Structured model for the <head> section of an HTML document.
    Stores metadata relevant for SEO and technical health.
    """
    tag: str = "head"

    # Title Metadata
    has_title: bool = False
    title_text: str = ""
    title_len: int = 0

    # Meta Description Metadata
    has_meta_desc: bool = False
    meta_desc_text: str = ""
    meta_desc_len: int = 0

    # Canonical Link Metadata
    has_canonical: bool = False
    canonical_href: str = ""

    # Technical Metadata
    has_charset: bool = False
    has_viewport: bool = False


def parse_head(tag: Tag, children: list) -> HeadElement:
    """
    Extracts high-level SEO metadata from a BeautifulSoup <head> tag.
    """
    title_tag = tag.find('title')
    meta_desc = tag.find('meta', attrs={'name': 'description'})
    canonical_link = tag.find('link', attrs={'rel': 'canonical'})
    charset = tag.find('meta', attrs={'charset': True})
    viewport = tag.find('meta', attrs={'name': 'viewport'})

    t_text = title_tag.get_text(strip=True) if title_tag else ""
    d_text = meta_desc.get('content', '').strip() if meta_desc else ""
    c_href = canonical_link.get('href', '').strip() if canonical_link else ""

    return HeadElement(
        tag="head",
        attrs=tag.attrs,
        children=children,
        has_title=bool(title_tag),
        title_text=t_text,
        title_len=len(t_text),
        has_meta_desc=bool(meta_desc),
        meta_desc_text=d_text,
        meta_desc_len=len(d_text),
        has_canonical=bool(canonical_link),
        canonical_href=c_href,
        has_charset=bool(charset),
        has_viewport=bool(viewport)
    )


# --- AUDIT RULES ---



@audit_spec(codes=["MISSING_TITLE", "EMPTY_TITLE", "TITLE_TOO_SHORT", "TITLE_TOO_LONG"])
def check_title(node: HeadElement) -> List[AuditResult]:
    """Validates the presence and length of the <title> tag."""
    res = []
    if not node.has_title:
        res.append(("MISSING_TITLE", "Document missing <title> tag", "title", "CRITICAL", "HEAD"))
    elif not node.title_text:
        res.append(("EMPTY_TITLE", "Title tag is present but empty", "title", "CRITICAL", "HEAD"))
    else:
        if node.title_len < 10:
            res.append((
                "TITLE_TOO_SHORT",
                f"Title too short ({node.title_len}): '{node.title_text}'",
                "title", "WARNING", "HEAD"
            ))
        elif node.title_len > 60:
            res.append((
                "TITLE_TOO_LONG",
                f"Title too long ({node.title_len}): '{node.title_text[:50]}...'",
                "title", "WARNING", "HEAD"
            ))
    return res


@audit_spec(codes=["MISSING_META_DESC", "EMPTY_META_DESC", "META_DESC_TOO_SHORT", "META_DESC_TOO_LONG"])
def check_meta_desc(node: HeadElement) -> List[AuditResult]:
    """Validates the presence and length of the meta description."""
    res = []
    if not node.has_meta_desc:
        res.append(("MISSING_META_DESC", "Document missing meta description", "meta_description", "WARNING", "HEAD"))
    elif not node.meta_desc_text:
        res.append(("EMPTY_META_DESC", "Meta description present but empty", "meta_description", "WARNING", "HEAD"))
    else:
        if node.meta_desc_len < 50:
            res.append((
                "META_DESC_TOO_SHORT",
                f"Meta desc too short ({node.meta_desc_len}): '{node.meta_desc_text}'",
                "meta_description", "INFO", "HEAD"
            ))
        elif node.meta_desc_len > 160:
            res.append((
                "META_DESC_TOO_LONG",
                f"Meta desc too long ({node.meta_desc_len}): '{node.meta_desc_text[:50]}...'",
                "meta_description", "INFO", "HEAD"
            ))
    return res


@audit_spec(codes=["MISSING_CANONICAL", "EMPTY_CANONICAL"])
def check_canonical(node: HeadElement) -> List[AuditResult]:
    """Ensures a canonical URL is defined to prevent duplicate content issues."""
    res = []
    if not node.has_canonical:
        res.append(("MISSING_CANONICAL", "Document missing canonical link", "canonical", "WARNING", "HEAD"))
    elif not node.canonical_href:
        res.append(("EMPTY_CANONICAL", "Canonical tag present but href is empty", "canonical", "WARNING", "HEAD"))
    return res


@audit_spec(codes=["MISSING_CHARSET", "MISSING_VIEWPORT"])
def check_technical(node: HeadElement) -> List[AuditResult]:
    """Checks for essential technical tags like charset and viewport."""
    res = []
    if not node.has_charset:
        res.append(("MISSING_CHARSET", "Document missing charset definition", "meta_charset", "CRITICAL", "HEAD"))
    if not node.has_viewport:
        res.append(("MISSING_VIEWPORT", "Document missing viewport tag", "meta_viewport", "CRITICAL", "HEAD"))
    return res


# --- ELEMENT DEFINITION ---
DEFINITION = ElementDefinition(
    tag_name="head",
    model=HeadElement,
    parser=parse_head,
    audit_rules=[check_title, check_meta_desc, check_canonical, check_technical]
)