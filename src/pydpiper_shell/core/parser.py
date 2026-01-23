# src/pydpiper_shell/core/parser.py
from __future__ import annotations
import re
import shlex
from typing import List, Optional, Tuple

# Define the operators and RegEx patterns here.
_OPS: set[str] = {"&&", "||", ";", "|"}
# Pattern to identify variable expansion: @{name}
VAR_PATTERN = re.compile(r"@\{([^}]+)\}")
# Pattern to identify the variable SET shorthand: @{name}=value
_SET_PATTERN = re.compile(r"^@\{([^}=]+)\}=(.*)$")
# Pattern to identify the variable GET shorthand: @{name} (full match)
_GET_PATTERN = re.compile(r"^@\{([A-Za-z_][\w\.]*)\}$")


def parse_command_line(line: str) -> List[Tuple[str, List[str], Optional[str]]]:
    """
    Parses the user input into a list of command segments (tuples).

    A command segment is defined as (command_name, args, op_before).
    Recognizes shorthands for 'set' (@{var}=value) and 'get' (@{var}).

    Args:
        line (str): The raw input string from the shell.

    Returns:
        List[Tuple[str, List[str], Optional[str]]]: List of command segments.
    """
    s = (line or "").strip()
    if not s:
        return []

    try:
        # Use shlex to handle quotes and complex arguments
        tokens = shlex.split(s, posix=True)
    except ValueError:
        # Fallback for simple input if shlex fails
        tokens = s.split()

    if not tokens:
        return []

    out: list[tuple[str, list[str], str | None]] = []
    current_name: str | None = None
    current_args: list[str] = []
    op_before: str | None = None
    i = 0

    def _flush(next_op: Optional[str] = None) -> None:
        """Appends the current command segment to the output list."""
        nonlocal current_name, current_args, op_before
        if current_name is not None:
            # The operator before the current command is stored in op_before
            out.append((current_name, current_args, op_before))
        current_name, current_args = None, []
        # The operator passed to flush becomes the op_before for the *next* command
        op_before = next_op

    while i < len(tokens):
        tok = tokens[i]

        # Check for operators
        if tok in _OPS:
            _flush(next_op=tok)
            i += 1
            continue

        # Start of a new command
        if current_name is None:
            # Check for set/get shorthands
            if '=' in tok and _SET_PATTERN.match(tok):
                current_name = "set"
                current_args.append(tok)
            elif _GET_PATTERN.fullmatch(tok):
                current_name = "get"
                current_args.append(tok)
            else:
                current_name = tok
        else:
            # Argument belongs to the current command
            current_args.append(tok)

        i += 1

    # Flush the last command segment
    _flush()

    # The final loop correction in the original code is no longer needed
    # as the operator is correctly tracked by `op_before`.
    return out