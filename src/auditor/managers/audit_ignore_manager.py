# src/auditor/managers/audit_ignore_manager.py
import json
import logging
from pathlib import Path
from typing import Set

logger = logging.getLogger(__name__)


class AuditIgnoreManager:
    """
    Beheert de lijsten met te negeren elementen (images, links) tijdens de audit,
    én de configuratie voor zichtbaarheid in rapporten (hidden issues).
    """

    def __init__(self, project_id: int, cache_dir: Path):
        self.project_id = project_id
        self.project_dir = cache_dir / str(project_id)
        self.project_dir.mkdir(parents=True, exist_ok=True)

        # Bestandsnamen
        self.img_ignore_file = self.project_dir / "imgignore.json"
        self.link_ignore_file = self.project_dir / "linkignore.json"
        self.issue_config_file = self.project_dir / "issue_config.json"  # <-- NIEUW

        # In-memory sets laden
        self.ignored_images: Set[str] = self._load(self.img_ignore_file)
        self.ignored_links: Set[str] = self._load(self.link_ignore_file)
        self.hidden_issues: Set[str] = self._load(self.issue_config_file)  # <-- NIEUW

    def _load(self, path: Path) -> Set[str]:
        """Laadt een JSON list en returned een Set."""
        if path.exists():
            try:
                with open(path, 'r') as f:
                    return set(json.load(f))
            except Exception as e:
                logger.warning(f"Kon ignore file niet lezen {path}: {e}")
        return set()

    def _save(self, path: Path, data: Set[str]):
        """Slaat een Set op als gesorteerde JSON list."""
        try:
            with open(path, 'w') as f:
                json.dump(list(sorted(data)), f, indent=2)
        except Exception as e:
            logger.error(f"Kon ignore file niet opslaan {path}: {e}")

    # --- Audit Run Ignore Logic (Images & Links) ---

    def update_ignore_list(self, type_: str, items: str = None, reset: bool = False) -> bool:
        """
        Update de configuratie voor img/link en sla direct op.
        """
        target_set = self.ignored_images if type_ == 'img' else self.ignored_links
        target_file = self.img_ignore_file if type_ == 'img' else self.link_ignore_file

        modified = False

        if reset:
            target_set.clear()
            modified = True

        if items:
            new_items = [i.strip() for i in items.split(',') if i.strip()]
            if new_items:
                target_set.update(new_items)
                modified = True

        if modified:
            self._save(target_file, target_set)

        return modified

    # --- Report Config Logic (Hidden Issues) ---

    def get_hidden_issues(self) -> Set[str]:
        """Geeft de set met issue codes terug die verborgen moeten worden in rapporten."""
        return self.hidden_issues

    def set_hidden_issues(self, hidden_codes: list):
        """Overschrijft de lijst met verborgen issues en slaat op."""
        self.hidden_issues = set(hidden_codes)
        self._save(self.issue_config_file, self.hidden_issues)