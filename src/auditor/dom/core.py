from typing import Dict, Any, List, Callable, Type, Optional, Tuple, Set
from pydantic import BaseModel, Field
from bs4 import Tag


def audit_spec(codes: List[str]):
    """
    Decorator to declare which issue codes a specific audit rule function returns.
    Facilitates auto-discovery by the DOMRegistry.
    """
    def decorator(func):
        func.defined_codes = codes
        return func
    return decorator


class ElementBase(BaseModel):
    """
    Base data model representing a generic DOM element in the simplified tree.
    """
    tag: str
    attrs: Dict[str, Any] = Field(default_factory=dict)
    text: Optional[str] = ""
    children: List['ElementBase'] = Field(default_factory=list)
    validation_errors: List[str] = Field(default_factory=list)
    status_code: Optional[int] = None
    is_external: bool = False

    @property
    def is_empty(self) -> bool:
        """Returns True if the element contains no text and no children."""
        return not self.text and not self.children


# Type alias for audit findings: (Code, Message, ElementType, Severity, Category)
AuditResult = Tuple[str, str, str, str, str]


class ElementDefinition:
    """
    Configuration object binding an HTML tag to its model, parser, and rules.
    """

    def __init__(
            self,
            tag_name: str,
            model: Type[ElementBase],
            parser: Callable[[Tag, List[ElementBase]], ElementBase],
            audit_rules: Optional[List[Callable[[Any], List[AuditResult]]]] = None,
            possible_codes: Optional[List[str]] = None
    ):
        self.tag_name = tag_name
        self.model = model
        self.parser = parser
        self.audit_rules = audit_rules or []

        # --- Auto-Discovery of Issue Codes ---
        final_codes: Set[str] = set(possible_codes or [])

        for rule in self.audit_rules:
            if hasattr(rule, 'defined_codes'):
                final_codes.update(rule.defined_codes)

        self.codes = sorted(list(final_codes))