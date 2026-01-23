# src/pydpiper_shell/core/handlers/get_handler.py
import re
from typing import List, Optional

# core.py is needed for the XNGINE import
from pydpiper_shell.core import core as shell_core
from pydpiper_shell.core.context.shell_context import ShellContext

_GET_PATTERN = re.compile(r"^@\{([A-Za-z_][\w\.]*)\}$")


def handle_get(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles the 'get' command, which is primarily used via the @{var} shorthand.

    Resolves and prints the value of a context variable.

    Args:
        args (List[str]): Arguments, where the first argument should be the variable name in @{name} format.
        ctx (ShellContext): The current shell context.
        _stdin (Optional[str]): Standard input (unused here).

    Returns:
        int: Exit code (0 for success, 1 for error or not found).
    """
    if not args:
        print("Usage: get @{name}")
        return 1

    token = args[0].strip()
    m = _GET_PATTERN.match(token)

    if not m:
        print(f"Invalid variable format: {token}. Must be in the format @{{name}}.")
        return 1

    key = m.group(1)

    # Use the XNGINE instance from core to resolve the variable value
    val = shell_core.XNGINE.resolve_var(key, ctx)

    if val is not None:
        print(val)
        return 0
    else:
        # Variable not found is treated as a non-zero exit code
        print(f"Error: Variable '@{{{key}}}' not found in context.")
        return 1