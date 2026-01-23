# src/auditor/dom/models.py
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from .core import ElementBase


class HTMLDocument(BaseModel):
    """
    Represents a parsed HTML document.

    This model serves as the root container for the DOM tree (head and body),
    document-level metadata, content analysis metrics (n-grams), and
    extracted structured data.
    """
    raw_url: str
    has_doctype: bool = False
    root_tag_valid: bool = False
    doc_errors: List[str] = Field(default_factory=list)

    # The DOM Tree Structure
    head: Optional[ElementBase] = None
    body: Optional[ElementBase] = None

    # Content Analysis Data
    body_text: str = ""
    body_unigrams: Dict[str, int] = Field(default_factory=dict)
    body_bigrams: Dict[str, int] = Field(default_factory=dict)
    body_trigrams: Dict[str, int] = Field(default_factory=dict)

    # --- Structured Data ---
    structured_data: List[Dict[str, Any]] = Field(default_factory=list)