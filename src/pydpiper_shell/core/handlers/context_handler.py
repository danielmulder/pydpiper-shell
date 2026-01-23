# src/pydpiper_shell/controllers/handlers/context_handler.py
import logging
from typing import List, Optional

from pydpiper_shell.core.context.shell_context import ShellContext

logger = logging.getLogger(__name__)

COMMAND_HIERARCHY = {
    "vars": None,
    "reset": None,
}

USAGE = """
Usage:
  context vars        Show all context variables
  context reset       Clear all context variables
""".strip()


def handle_context(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handle `context` commands (vars, reset).
    """
    if not args:
        print(USAGE)
        return 0

    cmd = args[0]

    # -------------------------------------------------------------- vars
    if cmd == "vars":
        if not ctx._vars:
            print("No context variables set.")
            return 0
        print("Shell context variables:")
        for k, v in ctx._vars.items():
            print(f"  {k} = {v}")
        return 0

    # -------------------------------------------------------------- reset
    if cmd == "reset":
        ctx._vars.clear()
        # CodePilot: Translated output message to English.
        print("✅ Context cleared.")
        return 0

    # -------------------------------------------------------------- unknown
    print(f"Unknown context command: {cmd}")
    return 1