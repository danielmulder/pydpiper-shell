# src/pydpiper_shell/core/managers/workflow_manager.py
import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from pydantic import TypeAdapter

from pydpiper_shell.core.managers.database_manager import DatabaseManager
from pydpiper_shell.model import Workflow

logger = logging.getLogger(__name__)


class WorkflowManager:
    """Manages the lifecycle and persistence of workflows in a dedicated global file."""

    _WORKFLOW_FILENAME = "_global_workflows.json"

    def __init__(self, db_mgr: DatabaseManager):
        self._workflow_file = db_mgr.base_dir / self._WORKFLOW_FILENAME
        self._workflow_list_adapter = TypeAdapter(List[Workflow])

    def load_all(self) -> List[Workflow]:
        """Loads all global workflows from the dedicated JSON file."""
        if not self._workflow_file.exists():
            return []
        try:
            return self._workflow_list_adapter.validate_json(self._workflow_file.read_bytes())
        except Exception as e:
            logger.error("Failed to load or parse workflows from %s: %s", self._workflow_file, e)
            return []

    def _save_all(self, workflows: List[Workflow]) -> None:
        """Atomically saves the entire list of workflows to the file."""
        try:
            sorted_workflows = sorted(workflows, key=lambda w: w.name.lower())
            json_bytes = self._workflow_list_adapter.dump_json(sorted_workflows, indent=2)

            temp_path = self._workflow_file.with_suffix(".tmp")
            temp_path.write_bytes(json_bytes)

            os.replace(temp_path, self._workflow_file)

        except Exception as e:
            logger.error("Failed to save workflows to %s: %s", self._workflow_file, e, exc_info=True)

    def save_workflow(self, workflow_to_save: Workflow) -> None:
        """
        Saves a workflow. If a workflow with the same name exists, it will be overwritten.
        """
        all_workflows = self.load_all()

        filtered_workflows = [
            w for w in all_workflows if w.name.lower() != workflow_to_save.name.lower()
        ]

        filtered_workflows.append(workflow_to_save)

        self._save_all(filtered_workflows)
        logger.info("Workflow '%s' saved/updated.", workflow_to_save.name)

    def delete_workflow(self, name: str) -> bool:
        """
        Deletes a workflow by its name (case-insensitive). Returns True on success.
        """
        all_workflows = self.load_all()

        workflows_to_keep = [
            w for w in all_workflows if w.name.lower() != name.lower()
        ]

        if len(workflows_to_keep) < len(all_workflows):
            self._save_all(workflows_to_keep)
            logger.info("Workflow '%s' deleted.", name)
            return True

        return False

    def find_by_name(self, name: str) -> Optional[Workflow]:
        """Finds a workflow by its unique name (case-insensitive)."""
        workflows = self.load_all()
        return next((w for w in workflows if w.name.lower() == name.lower()), None)