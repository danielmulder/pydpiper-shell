# src/pydpiper_shell/core/handlers/set_handler.py
import re
from typing import List, Optional

from pydpiper_shell.core.context.shell_context import ShellContext

# Regex pattern to match the @{key}=value format, capturing the key and the rest as value
_SET_PATTERN = re.compile(r"^@\{([^}=]+)\}=(.*)$")


def handle_set(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles the 'set' command, including the @{var}=value shorthand.

    Assigns a string value to a context variable.

    Args:
        args (List[str]): Arguments, where the key and value are expected
                          to be combined in the form '@{name}=value'.
        ctx (ShellContext): The current shell context where the variable will be stored.
        _stdin (Optional[str]): Standard input (unused here).

    Returns:
        int: Exit code (0 for success, 1 for usage error).
    """
    if not args:
        print("Usage: set @{name}=value")
        return 1

    # Rejoin arguments in case the user included spaces or used quotes
    # that shlex split might have separated.
    full_arg = " ".join(args)
    m = _SET_PATTERN.match(full_arg)

    if not m:
        print("Usage: set @{name}=value")
        return 1

    # Extract the key (group 1) and the value (group 2)
    key, value = m.group(1).strip(), m.group(2).strip()

    # Simple unquoting of the value if it starts and ends with double quotes
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]

    # Store the key-value pair in the shell context
    ctx.set(key, value)
    return 0