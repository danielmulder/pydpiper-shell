# src/pydpiper_shell/core/handlers/workflow_handler.py
import argparse
import logging
import re
from typing import List, Optional, Dict, Any

from pydpiper_shell.core import core as shell_core
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.workflow_manager import WorkflowManager
from pydpiper_shell.core.parser import parse_command_line
from pydpiper_shell.model import Workflow

logger = logging.getLogger(__name__)

workflow_help_text = """
WORKFLOWS:
  workflow list                   Show all saved command workflows.
  workflow run <name>             Execute a saved workflow by name.
  workflow <name>                 Shorthand for 'workflow run <name>'.
  workflow create "<cmds>" --name <name> [--description <text>]
                                  Create/update a workflow from a command string.
  workflow edit <name>            Loads a workflow's create command into the prompt.
  workflow delete <name>          Permanently deletes a saved workflow.
  !w                              For workflow autosuggest
""".strip()

# Keep known subcommands for parsing and auto-completion
KNOWN_SUBCOMMANDS = {"create", "list", "run", "edit", "delete"}
COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    sub: None for sub in KNOWN_SUBCOMMANDS
}

USAGE = """
Usage:
  workflow list
  workflow run <name>
  workflow <name>                 (Shorthand for run)
  workflow create "<cmd_string>" --name <name> [--description <text>]
  workflow edit <name>
  workflow delete <name>
  !w for workflow autosuggest
"""


def _normalize_command_string(command_string: str) -> str:
    """
    Normalizes the command string by removing line breaks and collapsing multiple spaces.
    This ensures that multi-line inputs are treated as a single valid command sequence.
    """
    # 1. Replace line breaks and tabs with single spaces
    normalized = re.sub(r'[\r\n\t]+', ' ', command_string)
    # 2. Replace sequences of multiple spaces with a single space
    normalized = re.sub(r'\s+', ' ', normalized)
    # 3. Trim leading and trailing whitespace
    return normalized.strip()


def _handle_create(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """Handles creating or updating a workflow."""
    parser = argparse.ArgumentParser(prog="workflow create")
    parser.add_argument("command_string")
    parser.add_argument("--name", required=True)
    parser.add_argument("--description", default="")
    try:
        parsed_args = parser.parse_args(args)
    except SystemExit:
        return 1

    manager = WorkflowManager(ctx.db_mgr)

    # Confirm overwrite if workflow exists
    if manager.find_by_name(parsed_args.name):
        confirm = input(
            f"⚠️ Workflow '{parsed_args.name}' already exists. Overwrite? [y/N]: "
        ).lower().strip()
        if confirm != 'y':
            print("Operation cancelled.")
            return 1

    new_workflow = Workflow(
        name=parsed_args.name,
        description=parsed_args.description,
        command_string=parsed_args.command_string.strip()
    )
    manager.save_workflow(new_workflow)
    print(f"✅ Workflow '{parsed_args.name}' created/updated.")
    return 0


def _handle_list(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """Handles listing all saved workflows."""
    manager = WorkflowManager(ctx.db_mgr)
    workflows = manager.load_all()
    if not workflows:
        print("No workflows found.")
        return 0

    print("\nAvailable Workflows:")
    print("-" * 70)
    for w in workflows:
        print(f"  Name: {w.name}")
        print(f"  Desc: {w.description or 'N/A'}")
        print(f"  CMD:  {w.command_string}")
        print("-" * 70)
    print()
    return 0


def _handle_run(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Handles running a workflow by name.
    Retrieves the stored command string, normalizes it, parses it, and executes it via XNGINE.
    """
    if not args:
        print("Usage: workflow run <name> OR workflow <name>")
        return 1

    workflow_name = args[0]  # The name is always the first argument here
    manager = WorkflowManager(ctx.db_mgr)
    workflow = manager.find_by_name(workflow_name)

    if not workflow:
        print(f"❌ Error: Workflow '{workflow_name}' not found.")
        return 1

    # Normalize the stored command string before execution
    normalized_command_string = _normalize_command_string(workflow.command_string)

    print(f"▶️ Running workflow '{workflow.name}': {normalized_command_string}")

    # Parse the normalized string into executable commands
    commands = parse_command_line(normalized_command_string)
    if not commands:
        print("⚠️ Workflow command string is empty or invalid.")
        return 1

    # Use the globally initialized XNGINE to execute the sequence
    exit_code = shell_core.XNGINE.execute_sequence(commands, ctx)
    print(f"✅ Workflow '{workflow.name}' finished with exit code {exit_code}.")
    return exit_code


def _handle_edit(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Finds a workflow and injects its 'create' command into the next prompt buffer.
    This allows the user to easily modify an existing workflow.
    """
    if not args:
        print("Usage: workflow edit <name>")
        return 1

    workflow_name = args[0]
    manager = WorkflowManager(ctx.db_mgr)
    workflow = manager.find_by_name(workflow_name)

    if not workflow:
        print(f"❌ Error: Workflow '{workflow_name}' not found.")
        return 1

    # Reconstruct the original create command programmatically
    description_part = f' --description "{workflow.description}"' if workflow.description else ""
    recreated_command = (
        f'workflow create "{workflow.command_string}" --name "{workflow.name}"{description_part}'
    )

    # Store the command in the context buffer for the next prompt iteration
    ctx.next_prompt_buffer = recreated_command

    print(f"✅ Workflow '{workflow.name}' loaded into your prompt for editing.")
    return 0


def _handle_delete(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """Finds and permanently deletes a workflow after user confirmation."""
    if not args:
        print("Usage: workflow delete <name>")
        return 1

    workflow_name = args[0]
    manager = WorkflowManager(ctx.db_mgr)

    if not manager.find_by_name(workflow_name):
        print(f"❌ Error: Workflow '{workflow_name}' not found.")
        return 1

    confirm = input(
        f"Are you sure you want to permanently delete workflow '{workflow_name}'? [y/N]: "
    ).lower().strip()

    if confirm != 'y':
        print("Operation cancelled.")
        return 1

    if manager.delete_workflow(workflow_name):
        print(f"✅ Workflow '{workflow_name}' has been deleted.")
        return 0
    else:
        # Fallback error handling
        print(f"❌ Failed to delete workflow '{workflow_name}'.")
        return 1


def handle_workflow(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Main handler for the 'workflow' command.
    Supports standard subcommands (create, list, run, etc.) and shorthand execution logic.
    """
    if not args:
        print(USAGE)
        return 1

    command_or_name = args[0]
    sub_args = args[1:]

    # Check if the first argument is a known subcommand
    if command_or_name in KNOWN_SUBCOMMANDS:
        match command_or_name:
            case "create":
                return _handle_create(sub_args, ctx, _stdin)
            case "list":
                return _handle_list([], ctx, _stdin)
            case "run":
                return _handle_run(sub_args, ctx, _stdin)
            case "edit":
                return _handle_edit(sub_args, ctx, _stdin)
            case "delete":
                return _handle_delete(sub_args, ctx, _stdin)
            case _:
                # Should be unreachable due to the KNOWN_SUBCOMMANDS check
                print(f"Internal error: Unhandled known subcommand '{command_or_name}'.")
                return 1
    else:
        # --- SHORTHAND LOGIC ---
        # If the argument is not a reserved subcommand, assume it is a workflow name.
        logger.debug(f"Interpreting '{command_or_name}' as workflow run shorthand.")
        return _handle_run([command_or_name], ctx, _stdin)