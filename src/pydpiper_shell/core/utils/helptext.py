# src/pydpiper_shell/core/utils/helptext.py
from pydpiper_shell.core.command_registry import COMMAND_HELP_TEXTS

# The static header part of the help text
HEADER_HELP_TEXT = """
🚀 PydPiper Mini Shell - Help

An interactive shell for managing and executing web crawls.

---
OPERATORS
---
  A ; B               Execute B after A, regardless of the outcome.
  A && B              Execute B only if A was successful (exit code 0).
  A || B              Execute B only if A failed (exit code != 0).
  A | B               Pipe the output (stdout) of A as input (stdin) for B.

---
VARIABLES & SHORTHANDS
---
  Variables are accessed using @{name}, e.g., @{project.id}.
  They are also expanded inside double quotes ("...").

  set @{name}=value   Create or overwrite a variable.
  Shorthand:          @{name}=value

  get @{name}         Display the value of a variable.
  Shorthand:          @{name}

  !h                  Trigger command history completion.

---
COMMANDS
---
GENERAL:
  help                Show this help text.
  quit                Exit the shell.
  cls                 Clear the screen.
  echo <text...>      Display the specified text.
  !w                  Workflow list with all workflows
  !h                  History list last n commands (Also see settings.json)
""".strip()


def get_help_text() -> str:
    """
    Dynamically assembles the full help text from the header and all
    discovered help text fragments from the command handlers.
    """
    full_help_parts = [HEADER_HELP_TEXT]

    # Sort the command help texts alphabetically for a consistent order
    for command_name in sorted(COMMAND_HELP_TEXTS.keys()):
        full_help_parts.append(COMMAND_HELP_TEXTS[command_name])

    return "\n\n".join(full_help_parts)