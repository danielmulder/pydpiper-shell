from typing import List, Any, Tuple
from bs4 import Tag
from ..core import ElementBase, ElementDefinition, AuditResult, audit_spec


class HeadingElement(ElementBase):
    """
    Model representing a heading element (h1-h6).
    Stores the heading level for structural analysis.
    """
    level: int


def parse_heading(tag: Tag, children: List[ElementBase]) -> HeadingElement:
    """
    Parses heading tags and determines their hierarchy level (e.g., h1 -> 1).
    """
    try:
        # Extract level from tag name (e.g., 'h1' -> 1)
        level = int(tag.name[1])
    except (ValueError, IndexError, TypeError):
        level = 0

    # Retrieve and strip text content
    text_content = tag.get_text(" ", strip=True)

    return HeadingElement(
        tag=tag.name,
        attrs=tag.attrs,
        text=text_content,
        children=children,
        level=level
    )


# --- AUDIT RULES ---



@audit_spec(codes=["EMPTY_H1"])
def check_h1_not_empty(node: HeadingElement) -> List[AuditResult]:
    """
    Rule: An H1 must not be empty.
    Validates that either text or an image (e.g., a logo) is present.
    """
    results = []

    if node.level == 1:
        # Check for children tags that might provide semantic value (like an <img> logo)
        has_img_child = any(child.tag == 'img' for child in node.children)

        if not node.text and not has_img_child:
            results.append((
                "EMPTY_H1",
                "H1 tag is empty (contains no text or image content)",
                "h1",
                "CRITICAL",
                "CONTENT"
            ))

    return results


# --- ELEMENT DEFINITION ---

# Note: The DOMRegistry currently maps one tag_name per DEFINITION.
# To handle h2-h6, we would typically register multiple definitions or
# modify the registry to accept a list of tags for a single definition.

DEFINITION = ElementDefinition(
    tag_name="h1",
    model=HeadingElement,
    parser=parse_heading,
    audit_rules=[check_h1_not_empty]
)