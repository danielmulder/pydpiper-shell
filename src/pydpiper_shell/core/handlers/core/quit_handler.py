# src/pydpiper_shell/core/handlers/quit_handler.py
from pydpiper_shell.core.context.shell_context import ShellContext

def handle_quit(_args, _ctx: ShellContext, _stdin=None) -> int:
    """Signals the shell to stop."""
    return 130  # Special exit code for 'quit'