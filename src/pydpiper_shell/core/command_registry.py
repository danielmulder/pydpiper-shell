# src/pydpiper_shell/core/command_registry.py
import logging
from typing import Callable, Dict, Any

from pydpiper_shell.core.discovery import discover_handlers

logger = logging.getLogger(__name__)

# The central registries, populated dynamically.
CommandRegistry: Dict[str, Callable[..., int]] = {}
COMMAND_HIERARCHY: Dict[str, Any] = {}
COMMAND_HELP_TEXTS: Dict[str, str] = {}


def register_command(name: str, handler: Callable[..., int]) -> None:
    """Adds a command and its handler function to the registry."""
    CommandRegistry[name] = handler
    logger.debug("Registered command '%s'", name)


def register_all_commands() -> None:
    """
    Discovers all handlers, hierarchies, and help texts, then registers them.
    """
    logger.debug("Discovering all command handlers, hierarchies, and help texts...")

    # The discovery now returns three dictionaries
    discovered_handlers, discovered_hierarchies, discovered_help_texts = discover_handlers()

    # Register every discovered command handler
    for name, handler in discovered_handlers.items():
        if name not in CommandRegistry:
            register_command(name, handler)

    # Build the main hierarchy and help texts from the discovered parts
    COMMAND_HIERARCHY.update(discovered_hierarchies)
    COMMAND_HELP_TEXTS.update(discovered_help_texts)

    # Add commands without subcommands to the hierarchy for the completer
    for name in CommandRegistry.keys():
        if name not in COMMAND_HIERARCHY:
            COMMAND_HIERARCHY[name] = None

    logger.debug("Successfully registered %d handlers and built command hierarchy.", len(CommandRegistry))