# src/pydpiper_shell/core/services/workflow_persistence_service.py
import logging
from typing import List, Optional

from pydpiper_shell.core.managers.database_manager import DatabaseManager
from pydpiper_shell.model import Workflow

logger = logging.getLogger(__name__)


class WorkflowPersistenceService:
    """
    Manages the persistence of reusable command workflows.

    Workflows are stored as a list in a JSON-based cache file associated
    with the global project ID (0), allowing them to be shared across the system.
    """

    _GLOBAL_PROJECT_ID = 0
    _CACHE_NAME = "_workflows"

    def __init__(self, db_mgr: DatabaseManager):
        self._db_mgr = db_mgr

    def load_all(self) -> List[Workflow]:
        """
        Loads all global workflows from the persistent cache.

        Returns:
            List[Workflow]: A list of all available workflows, or an empty list if loading fails.
        """
        try:
            # Load the list of Workflow objects from the JSON cache
            return self._db_mgr.load_cache(
                self._GLOBAL_PROJECT_ID, self._CACHE_NAME, Workflow
            )
        except Exception as e:
            logger.error(f"Failed to load workflows: {e}")
            return []

    def save_workflow(self, workflow: Workflow) -> bool:
        """
        Saves a new workflow to the store.

        This method loads existing workflows, checks for naming collisions,
        appends the new workflow, and overwrites the cache file with the updated list.

        Args:
            workflow: The Workflow object to save.

        Returns:
            bool: True if saved successfully, False if a duplicate name exists.
        """
        # Step 1: Load all existing workflows
        all_workflows = self.load_all()

        # Step 2: Check for duplicate names (case-insensitive)
        if any(w.name.lower() == workflow.name.lower() for w in all_workflows):
            logger.warning(
                "Attempted to create a workflow with a duplicate name: %s",
                workflow.name
            )
            print(f"❌ Error: Workflow with name '{workflow.name}' already exists.")
            return False

        # Step 3: Append the new workflow
        all_workflows.append(workflow)

        # Step 4: Persist the updated list
        # The save_cache method overwrites the existing file with the new list.
        try:
            self._db_mgr.save_cache(
                self._GLOBAL_PROJECT_ID, self._CACHE_NAME, all_workflows
            )
            logger.info("Workflow '%s' saved successfully.", workflow.name)
            return True
        except Exception as e:
            logger.error(f"Failed to save workflow '{workflow.name}': {e}")
            return False

    def find_by_name(self, name: str) -> Optional[Workflow]:
        """
        Finds a specific workflow by its unique name (case-insensitive).

        Args:
            name: The name of the workflow to find.

        Returns:
            Optional[Workflow]: The found Workflow object, or None if not found.
        """
        workflows = self.load_all()
        return next(
            (w for w in workflows if w.name.lower() == name.lower()),
            None
        )