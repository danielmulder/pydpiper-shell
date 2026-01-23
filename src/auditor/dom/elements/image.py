from typing import List, Optional
from bs4 import Tag
from ..core import ElementBase, ElementDefinition, AuditResult, audit_spec


class ImageElement(ElementBase):
    tag: str = "img"

    @property
    def src(self) -> str: return self.attrs.get('src', '')

    @property
    def alt(self) -> Optional[str]: return self.attrs.get('alt')


def parse_image(tag: Tag, children: list) -> ImageElement:
    return ImageElement(tag="img", attrs=tag.attrs, children=children)


# --- RULES ---

@audit_spec(codes=["MISSING_ALT", "EMPTY_ALT"])
def check_alt_text(node: ImageElement) -> List[AuditResult]:
    res = []
    # alt=None means the attribute is missing
    if node.alt is None:
        res.append(("MISSING_ALT", f"Image missing alt attribute: {node.src}", "image", "CRITICAL", "CONTENT"))
    # alt="" means the attribute is empty (decorative?)
    elif not node.alt.strip():
        res.append(("EMPTY_ALT", f"Image has empty alt text: {node.src}", "image", "WARNING", "CONTENT"))

    return res


@audit_spec(codes=["MISSING_SRC", "CRITICAL_BASE64_BLOAT", "LARGE_BASE64_IMG"])
def check_source(node: ImageElement) -> List[AuditResult]:
    res = []

    if not node.src:
        res.append(("MISSING_SRC", "Image tag has no source", "image", "CRITICAL", "TECHNICAL"))
        return res

    # Logic for Base64 Performance Impact
    if node.src.startswith("data:image"):
        # Calculate size in Kilobytes (len(str) is a sufficient proxy for bytes here)
        size_in_bytes = len(node.src)
        size_in_kb = size_in_bytes / 1024

        WARNING_THRESHOLD = 20  # 20KB
        CRITICAL_THRESHOLD = 100  # 100KB

        if size_in_kb > CRITICAL_THRESHOLD:
            res.append((
                "CRITICAL_BASE64_BLOAT",
                f"Critical HTML Bloat: Base64 image is {round(size_in_kb, 2)}KB. Prevents caching & slows TTFB.",
                "image",
                "CRITICAL",
                "PERFORMANCE"
            ))
        elif size_in_kb > WARNING_THRESHOLD:
            res.append((
                "LARGE_BASE64_IMG",
                f"Large Base64 image detected ({round(size_in_kb, 2)}KB). Consider using an external file.",
                "image",
                "WARNING",
                "PERFORMANCE"
            ))

        # If < 20KB, it's considered an optimization (Pass), so we return nothing.

    return res


# --- DEFINITION ---
DEFINITION = ElementDefinition(
    tag_name="img",
    model=ImageElement,
    parser=parse_image,
    audit_rules=[check_alt_text, check_source]
)