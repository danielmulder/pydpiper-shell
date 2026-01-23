# src/pydpiper_shell/core/handlers/strict_handler.py
from typing import List, Optional, Dict, Any

from pydpiper_shell.core.context.shell_context import ShellContext

# Define the hierarchy for auto-suggestion
COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "status": None,
    "override": None,
}

USAGE = """
Usage:
  strict status        Show the current strict mode status.
  strict override      Disable strict mode for the current session.
"""


def handle_strict(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles the 'strict' command, controlling the Pydantic validation strict mode
    for the current session's crawler.

    NOTE: This handler assumes that `ctx.strict_mode` exists and is a boolean
    attribute on the ShellContext object.
    """
    if not args:
        print(USAGE)
        return 1

    command = args[0]

    if command == "status":
        # Check the current status of the strict mode
        # Assuming ctx.strict_mode is initialized to True (ON) by default
        status = "ON" if getattr(ctx, 'strict_mode', True) else "OFF"
        print(f"Pydantic strict mode is currently: {status}")

        if not getattr(ctx, 'strict_mode', True):
            print("Malformatted URLs will be ignored during crawling.")
        return 0

    if command == "override":
        # Disable strict mode for the session
        setattr(ctx, 'strict_mode', False)
        print("✅ Strict mode has been disabled for the current session.")
        print("   The crawler will now be more permissive with malformatted URLs.")
        return 0

    print(f"Unknown command: 'strict {command}'.")
    print(USAGE)
    return 1