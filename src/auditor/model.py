from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator
import json


class AuditIssue(BaseModel):
    """
    Data model representing a single finding from the SEO/Technical audit.
    Maps directly to the 'audit_issues' table in the SQLite database.
    """
    project_id: int
    page_id: int
    url: str

    # Classification
    category: str  # e.g., 'CONTENT', 'TECHNICAL', 'HEAD', 'LINKS'
    element_type: str  # e.g., 'image', 'title', 'meta_description', 'anchor'
    issue_code: str  # e.g., 'MISSING_ALT', 'TITLE_TOO_LONG', 'BROKEN_LINK'
    severity: str  # 'CRITICAL', 'WARNING', 'INFO'

    # Content
    message: str  # Human-readable description of the issue
    details: Optional[Dict[str, Any]] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('details', mode='before')
    @classmethod
    def parse_details(cls, v: Any) -> Dict[str, Any]:
        """
        Ensures the 'details' field is a dictionary.
        Automatically parses JSON strings if the data is coming from a raw SQL result.
        """
        if isinstance(v, str):
            try:
                return json.loads(v)
            except (json.JSONDecodeError, TypeError):
                return {}
        return v or {}

    class Config:
        # Allows the model to be populated using object attributes (e.g., from SQLAlchemy or Row objects)
        from_attributes = True