# src/pydpiper_shell/core/handlers/plugin_handler.py
from typing import List, Optional, Dict, Any

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.plugins.manager import PluginManager

plugin_help_text = """
PLUGINS:
  plugin list         List all available plugins.
  plugin run <name>   Execute a specific plugin.
""".strip()

# Define the hierarchy for auto-suggestion
COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "list": None,
    "run": None,
}

USAGE = """
Usage:
  plugin list
  plugin run <plugin_name> [args...]
"""

def handle_plugin(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles discovery and execution of plugins.

    Args:
        args (List[str]): Command line arguments passed to the plugin handler.
        ctx (ShellContext): The current shell context.
        _stdin (Optional[str]): Standard input passed via pipeline (unused here).

    Returns:
        int: Exit code (0 for success, 1 for usage error, plugin's exit code otherwise).
    """
    manager = PluginManager()

    if not args:
        print(USAGE)
        return 1

    command = args[0]
    plugin_args = args[1:]

    if command == "list":
        # Discover and list all available plugins
        plugins = manager.discover_plugins()
        if not plugins:
            print("No plugins found in the 'plugins' directory.")
            return 0

        print("Available plugins:")
        for name in plugins:
            print(f"  - {name}")
        return 0

    if command == "run":
        # Execute a specific plugin
        if not plugin_args:
            print("Usage: plugin run <plugin_name> [args...]")
            return 1

        plugin_name = plugin_args[0]
        extra_args = plugin_args[1:]

        # Delegate execution to the PluginManager
        return manager.run_plugin(plugin_name, extra_args, ctx)

    print(f"Unknown plugin command: '{command}'")
    return 1