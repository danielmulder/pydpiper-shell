# src/pydpiper_shell/core/handlers/opt_handler.py
import argparse
from typing import List, Optional, Dict, Any

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.shell_history_manager import ShellHistoryManager

opt_help_text = """
OPTIMIZER:
  opt history [--review-xl <N>] [--opt-potential]
                      Deduplicates the shell command history.
                      --review-xl <N> signals commands with >N tokens for interactive removal.
                      --opt-potential  Analyzes command length distribution to report on optimization potential.
""".strip()

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "history": None,
}

USAGE = """
Usage:
  opt history
  opt history --review-xl [<N>]
  opt history --opt-potential
"""

def handle_opt(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles the 'opt' command by dispatching tasks to the ShellHistoryManager.
    This handler is responsible for parsing arguments and delegating the main logic.
    """
    if not args or args[0] != 'history':
        print(f"Unknown subcommand for 'opt'.\n{USAGE}")
        return 1

    parser = argparse.ArgumentParser(prog="opt history", description="Optimize the command history.", usage=USAGE)
    parser.add_argument(
        "--review-xl",
        type=int,
        nargs='?',
        const=True,
        default=None,
        help="Flag commands with >N tokens for interactive review (omitting N uses dynamic threshold)."
    )
    parser.add_argument(
        "--opt-potential",
        action='store_true',
        help="Analyze command length distribution and report on optimization potential (no changes are made)."
    )

    try:
        parsed_args = parser.parse_args(args[1:])
    except SystemExit:
        # Argparse handles showing help and exiting, which is fine.
        return 1
    except argparse.ArgumentError as e:
        print(f"Error: {e}")
        return 1

    # Instantiate the manager and delegate the task
    manager = ShellHistoryManager(ctx)
    return manager.optimize(
        review_xl_input=parsed_args.review_xl,
        opt_potential_report=parsed_args.opt_potential
    )