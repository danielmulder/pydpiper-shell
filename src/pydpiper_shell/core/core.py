# src/pydpiper_shell/core/core.py
from __future__ import annotations

import logging
from typing import List

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.xngine import ExecuteEngine
from pydpiper_shell.core.command_registry import CommandRegistry
from pydpiper_shell.core.parser import VAR_PATTERN, parse_command_line

logger = logging.getLogger(__name__)


# The call to register_all_commands() has been removed from this module.
# Registration is now handled exclusively in app.py, which is the correct
# location and prevents circular import issues.


def _maybe_expand_args(name: str, args: list[str], ctx: ShellContext) -> list[str]:
    """Helper that expands arguments before they are passed to a handler."""
    return [XNGINE.expand_context_vars(a, ctx) for a in args]


def _post_refresh(ctx: ShellContext) -> None:
    """Callback after every command (currently not used)."""
    pass


# Initialize the engine with the registered commands and parser logic.
XNGINE = ExecuteEngine(
    command_registry=CommandRegistry,
    var_pattern=VAR_PATTERN,
    maybe_expand_args=_maybe_expand_args,
    post_refresh=_post_refresh,
    logger=logger,
)

# Export core functionality for use by the main application layer.
execute_sequence = XNGINE.execute_sequence
expand_context_vars = XNGINE.expand_context_vars

# Export the necessary functions so app.py and other modules can use them.
__all__ = ["execute_sequence", "expand_context_vars", "parse_command_line"]