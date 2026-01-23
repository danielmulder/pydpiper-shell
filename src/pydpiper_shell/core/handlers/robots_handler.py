# src/pydpiper_shell/core/handlers/robots_handler.py
from typing import List, Optional, Dict, Any

from pydpiper_shell.core.context.shell_context import ShellContext
# --- UPDATED IMPORT ---
from pydpiper_shell.core.managers.config_manager import config_manager

robots_help_text = """
  robots status       Show if robots.txt is being respected.
  robots enable       Enable respecting robots.txt for the session.
  robots disable      Disable respecting robots.txt for the session.
""".strip()

# Definieer de hiërarchie voor auto-suggestie
COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "status": None,
    "enable": None,
    "disable": None,
}

USAGE = """
Usage:
  robots status        Show the current robots.txt respect status.
  robots enable        Enable respecting robots.txt for the current session.
  robots disable       Disable respecting robots.txt for the current session.
"""


def handle_robots(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles the 'robots' command to control whether the crawler
    respects robots.txt for the current session.
    """
    if not args:
        print(USAGE)
        return 1

    command = args[0]

    if command == "status":
        # --- UPDATED USAGE ---
        is_enabled = config_manager.get_nested('robots_txt.enabled', False)
        status = "ON" if is_enabled else "OFF"
        print(f"Respecting robots.txt is currently: {status}")
        if is_enabled:
            print("   The crawler will be blocked by robots.txt rules.")
        else:
            print("   The crawler will ignore robots.txt rules.")
        return 0

    if command == "enable":
        # --- UPDATED USAGE ---
        config_manager.set_nested('robots_txt.enabled', True)
        print("✅ Respect for robots.txt has been ENABLED for the current session.")
        return 0

    if command == "disable":
        # --- UPDATED USAGE ---
        config_manager.set_nested('robots_txt.enabled', False)
        print("✅ Respect for robots.txt has been DISABLED for the current session.")
        return 0

    print(f"Unknown command: 'robots {command}'.")
    print(USAGE)
    return 1