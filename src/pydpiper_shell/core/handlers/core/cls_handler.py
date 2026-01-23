# src/pydpiper_shell/controllers/handlers/cls_handler.py
import os
import platform
from typing import List, Optional

from pydpiper_shell.core.context.shell_context import ShellContext


def handle_cls(_args: List[str], _ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Clear the terminal screen (like `cls` on Windows or `clear` on Unix).
    """
    system = platform.system().lower()
    try:
        if "windows" in system:
            os.system("cls")
        else:
            os.system("clear")
        return 0
    except Exception as e:
        print(f"Failed to clear screen: {e}")
        return 1
