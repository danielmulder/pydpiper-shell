# src/pydpiper_shell/core/handlers/history_handler.py
import argparse
from typing import List, Optional, Dict, Any

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.shell_history_manager import ShellHistoryManager

history_help_text = """
HISTORY MANAGEMENT:
  history reset [--to <spec>]   Resets the command history.
                                --to +N: deletes the first N commands.
                                --to -N: deletes the last N commands.
  history backup [--rollback]   Creates a backup of the history file, or restores it.
  history info                Displays statistics about the command history.
""".strip()

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "reset": None,
    "backup": None,
    "info": None,
}

USAGE = """
Usage:
  history reset [--to <spec>]
  history backup [--rollback]
  history info
"""


def handle_history(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles the 'history' command for direct management tasks.
    """
    if not args:
        print(USAGE)
        return 1

    subcommand = args[0]
    manager = ShellHistoryManager(ctx)

    if subcommand == 'reset':
        parser = argparse.ArgumentParser(prog="history reset", description="Reset the command history.", usage=USAGE)
        parser.add_argument(
            "--to",
            dest="spec",
            type=str,
            default=None,
            help="Specify which commands to delete."
        )
        try:
            return manager.reset(spec=parser.parse_args(args[1:]).spec)
        except SystemExit:
            return 1

    elif subcommand == 'backup':
        parser = argparse.ArgumentParser(prog="history backup", description="Backup or rollback history.", usage=USAGE)
        parser.add_argument(
            "--rollback",
            action="store_true",
            help="Restore the history from the last backup."
        )
        try:
            parsed_args = parser.parse_args(args[1:])
            return manager.rollback() if parsed_args.rollback else manager.backup()
        except SystemExit:
            return 1

    elif subcommand == 'info':
       return manager.display_info()

    else:
        print(f"Unknown subcommand for 'history': {subcommand}.\n{USAGE}")
        return 1