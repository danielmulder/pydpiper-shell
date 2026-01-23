from __future__ import annotations

import logging
import sys
import asyncio

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer
from prompt_toolkit.document import Document
from prompt_toolkit.history import FileHistory

from pydpiper_shell.core.command_registry import COMMAND_HIERARCHY, register_all_commands
from pydpiper_shell.core.filter_registry import register_all_filters
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.core import (
    execute_sequence,
    parse_command_line,
)
from pydpiper_shell.core.loop_runner import ensure_background_loop
from pydpiper_shell.core.managers.project_manager import ProjectManager
from pydpiper_shell.core.managers.config_manager import config_manager
from pydpiper_shell.core.managers.completion_manager import CompletionManager
from pydpiper_shell.core.utils.configure_logging import configure_logger
from pydpiper_shell.core.utils.path_utils import PathUtils

# Initialize logging based on configuration
DEBUG_LEVEL = config_manager.get_nested("debug.level")
configure_logger(DEBUG_LEVEL)
logger = logging.getLogger(__name__)


def _setup_windows_event_loop_if_needed() -> None:
    """Installs Windows compatible asyncio policy if possible."""
    if not sys.platform.startswith("win"):
        return
    try:
        policy = asyncio.WindowsSelectorEventLoopPolicy()
        asyncio.set_event_loop_policy(policy)
        logger.info("Using WindowsSelectorEventLoopPolicy for asyncio on Windows.")
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not set WindowsSelectorEventLoopPolicy: %s", exc)


class PromptToolkitCompleter(Completer):
    """
    A wrapper that uses the CompletionManager to generate suggestions
    in a way that prompt_toolkit expects.
    """

    def __init__(self, manager: CompletionManager):
        self.manager = manager

    def get_completions(self, document: Document, complete_event):
        # Delegate the actual completion logic to the CompletionManager
        yield from self.manager.generate_completions(document)


# --- Shell Application ---


def start_shell() -> None:
    """Starts the interactive REPL (Read-Eval-Print Loop) for the PydPiper shell."""
    _setup_windows_event_loop_if_needed()
    register_all_commands()
    register_all_filters()
    ensure_background_loop()
    logger.debug("Background asyncio event loop is running.")

    ctx = ShellContext()
    try:
        logger.debug("Initializing project manager...")
        ctx.project_manager = ProjectManager(ctx.db_manager)
        logger.debug(
            "Found %d existing projects.",
            len(ctx.project_manager.get_all_projects())
        )
    except Exception as e:
        logger.error("Failed to initialize ProjectManager: %s", e, exc_info=True)
        print("\nFATAL: Could not initialize ProjectManager. Exiting.")
        return

    print("Welcome to PydPiper Shell 1.0 (type 'help' for commands)")

    # Setup history and completion
    history_path = PathUtils.get_shell_history_file()
    history = FileHistory(str(history_path))

    completion_manager = CompletionManager(ctx, history, COMMAND_HIERARCHY)
    prompt_completer = PromptToolkitCompleter(completion_manager)

    session = PromptSession(
        history=history,
        completer=prompt_completer,
        complete_while_typing=True
    )

    ctx.prompt_session = session
    logger.info("Shell startup; history file at: %s", history_path)

    try:
        while True:
            try:
                # Handle prompt buffer (e.g., if a command suggests the next input)
                default_text = ctx.next_prompt_buffer or ""
                if ctx.next_prompt_buffer:
                    ctx.next_prompt_buffer = None

                line = session.prompt("PydPiper>> ", default=default_text).strip()
            except (EOFError, KeyboardInterrupt):
                break

            if not line:
                continue

            # Parse input and execute the command sequence
            commands = parse_command_line(line)
            if not commands:
                continue

            code = execute_sequence(commands, ctx)
            if code == 130:  # Explicit Quit signal from engine
                break
    finally:
        print("Bye!")


def main(argv: list[str] | None = None) -> int:
    """Entrypoint for running the shell from the command line."""
    start_shell()
    return 0


if __name__ == "__main__":
    sys.exit(main())