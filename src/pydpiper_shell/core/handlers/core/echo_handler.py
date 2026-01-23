# src/pydpiper_shell/core/handlers/core/echo_handler.py
from typing import List, Optional

from pydpiper_shell.core.context.shell_context import ShellContext


def handle_echo(args: List[str], _ctx: ShellContext, stdin: Optional[str] = None) -> int:
    """
    Handles the 'echo' command.

    Prints the provided text and returns a specific exit code if the
    --code flag is used.

    Args:
        args (List[str]): Arguments passed to the echo command.
        _ctx (ShellContext): The shell context (unused in this handler).
        stdin (Optional[str]): Standard input piped from a previous command.

    Returns:
        int: The specified exit code (default is 0).
    """
    text_to_print_args = []
    exit_code = 0
    i = 0

    # Manually parse arguments to find the optional '--code <N>' flag
    while i < len(args):
        arg = args[i]

        if arg == "--code":
            if i + 1 < len(args):
                try:
                    # Attempt to convert the next parameter to an integer exit code
                    exit_code = int(args[i + 1])
                    i += 1  # Skip the next parameter as it was processed as the code value
                except ValueError:
                    # If it's not a number, treat the '--code' flag itself as normal text
                    text_to_print_args.append(arg)
            else:
                # --code was the last argument, so treat it as text
                text_to_print_args.append(arg)
        else:
            # All other arguments are collected as text to be printed
            text_to_print_args.append(arg)

        i += 1

    # Determine what should be printed: joined arguments or piped stdin
    s = " ".join(text_to_print_args) if text_to_print_args else (stdin or "")
    print(s)

    # Return the (potentially modified) exit code
    return exit_code