# src/pydpiper_shell/core/handlers/help_handler.py
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.utils.helptext import get_help_text

def handle_help(_args, _ctx: ShellContext, _stdin=None) -> int:
    print(get_help_text())
    return 0