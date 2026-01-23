# src/pydpiper_shell/core/handlers/config_handler.py
import json
from typing import List, Optional, Dict, Any

import logging

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.config_manager import config_manager

logger = logging.getLogger(__name__)

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "list": None,
    "set": None,
    "reset": None,
}

# --- TOEGEVOEGDE USAGE TEKST ---
USAGE = """
Usage:
  config list                Show the current configuration as JSON.
  config set <key> <value>   Set a config value for the session (e.g., debug.level INFO).
  config reset               Reload the configuration from settings.json.
"""


def handle_config(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """Handles the 'config' command for viewing and modifying session configuration."""
    if not args:
        print(USAGE)
        return 1

    command = args[0]

    if command == "list":
        config_data = config_manager.get_all()
        print(json.dumps(config_data, indent=2))
        return 0

    if command == "set":
        if len(args) < 3:
            print("Usage: config set <key> <value>")
            return 1
        key_path = args[1]
        value = " ".join(args[2:])

        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]

        if config_manager.set_nested(key_path, value):
            new_value = config_manager.get_nested(key_path)
            print(f"✅ Config updated: {key_path} = {new_value} (type: {type(new_value).__name__})")
            return 0
        else:
            print(f"❌ Error: Failed to set config value for key '{key_path}'.")
            return 1

    if command == "reset":
        config_manager.reset()
        print("✅ Configuration has been reset to the values from settings.json.")
        return 0

    print(f"Unknown command: 'config {command}'.")
    return 1