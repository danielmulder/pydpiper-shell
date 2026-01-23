# src/pydpiper_shell/model.py (Shell Layer)
from datetime import datetime, timezone

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class Project(BaseModel):
    id: int
    name: str
    start_url: str
    run_mode: str = "discovery"
    sitemap_url: Optional[str] = None
    total_time: float = 0.0
    pages: int = 0
    created_at: datetime = Field(default_factory=datetime.now)


class Workflow(BaseModel):
    name: str = Field(description="Unique name for the workflow.")
    description: str = Field(description="A brief explanation of what the workflow does.", default="")
    command_string: str = Field(description="The complete command chain string.")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class HistoryEntry(BaseModel):
    """
    Represents a single command entry in the prompt_toolkit history file,
    including the command text and the metadata timestamp.
    """
    command: str = Field(description="The executed command text.")
    timestamp: datetime = Field(description="The execution timestamp.")

    def format_for_file(self) -> str:
        """
        Formats the entry back into the prompt_toolkit FileHistory format:
        +<command>\n# <timestamp>\n\n
        """
        # Timestamp must be exact and newline='\n' is crucial for clean line endings
        return (
            f"+{self.command}\n"
            f"# {self.timestamp.isoformat()}\n"  
            f"\n"
        )