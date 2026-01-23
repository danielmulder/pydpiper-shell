from __future__ import annotations

import io
import inspect
import logging
import subprocess
import re
from contextlib import redirect_stdout
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple

try:
    from pydpiper_shell.core.context.shell_context import ShellContext
except Exception:
    ShellContext = object  # type: ignore

class ExecuteEngine:
    """
    Core engine responsible for command execution, operator handling,
    and context variable expansion. Now includes automatic DB housekeeping.
    """

    def __init__(
            self,
            *,
            command_registry: Dict[str, Callable[..., int]],
            var_pattern: Pattern[str],
            maybe_expand_args: Callable[[str, List[Any], ShellContext], List[str]],
            post_refresh: Callable[[ShellContext], None],
            parse_fn: Optional[Callable[[str], List[Tuple[str, List[str], Optional[str]]]]] = None,
            logger: Optional[logging.Logger] = None,
    ) -> None:
        self._commands = command_registry
        self._VAR_PATTERN = var_pattern
        self._maybe_expand_args = maybe_expand_args
        self._post_refresh = post_refresh
        self._parse = parse_fn
        self._log = logger or logging.getLogger(__name__)

    def expand_context_vars(self, text: str, ctx: ShellContext) -> str:
        """Performs @{var} expansion in the given text."""
        def repl(m: re.Match) -> str:
            end = m.end()
            if end < len(text) and text[end] == '=':
                return m.group(0)
            name = m.group(1)
            val = self.resolve_var(name, ctx)
            return str(val) if val is not None else m.group(0)

        return self._VAR_PATTERN.sub(repl, text)

    def execute_sequence(
            self,
            commands: List[Tuple[str, List[str], Optional[str]]],
            context: Optional[ShellContext] = None
    ) -> int:
        """
        Executes a sequence of commands and performs automatic
        database housekeeping upon completion.
        """
        ctx = context or ShellContext()  # type: ignore[call-arg]
        if not commands:
            return 0

        last_exit = 0
        i = 0
        n = len(commands)

        try:
            while i < n:
                name, raw_args, op = commands[i]

                # --- Operator Logic (&&, ||) ---
                if op == "&&" and last_exit != 0:
                    i += 1
                    while i < n and commands[i][2] == "|":
                        i += 1
                    continue

                if op == "||" and last_exit == 0:
                    i += 1
                    while i < n and commands[i][2] == "|":
                        i += 1
                    continue

                # --- Pipeline Handling ---
                segment: List[Tuple[str, List[str]]] = []
                j = i
                segment.append((name, raw_args))
                j += 1
                while j < n and commands[j][2] == "|":
                    segment.append((commands[j][0], commands[j][1]))
                    j += 1

                stdin: Optional[str] = None
                for k, (seg_name, seg_raw_args) in enumerate(segment):
                    is_last = (k == len(segment) - 1)
                    seg_args = self._maybe_expand_args(seg_name, seg_raw_args, ctx)

                    # --- Shorthand Get Variable ---
                    m = self._VAR_PATTERN.fullmatch(seg_name)
                    if m:
                        key = m.group(1)
                        val = self.resolve_var(key, ctx)
                        print(val if val is not None else f"@{'{'}{key}{'}'} not set")
                        last_exit = 0
                        self._post_refresh(ctx)
                        continue

                    handler = self._commands.get(seg_name)

                    # --- Command Routing (Internal vs External) ---
                    if handler is None:
                        if not is_last:
                            buf = io.StringIO()
                            with redirect_stdout(buf):
                                exit_code = self._run_external(seg_name, seg_args, stdin)
                            stdin = buf.getvalue()
                        else:
                            exit_code = self._run_external(seg_name, seg_args, stdin)
                        last_exit = int(exit_code)
                    else:
                        if not is_last:
                            buf = io.StringIO()
                            with redirect_stdout(buf):
                                exit_code = self._call_handler(handler, seg_args, ctx, stdin)
                            stdin = buf.getvalue()
                        else:
                            exit_code = self._call_handler(handler, seg_args, ctx, stdin)
                        last_exit = int(exit_code)

                    self._post_refresh(ctx)
                    if last_exit == 130:
                        return 130
                    if last_exit != 0 and not is_last:
                        stdin = None
                        break

                i = j
        finally:
            # --- HOUSEKEEPING: Dit is de cruciale toevoeging ---
            # Zodra de sequence klaar is (of crasht), ruimen we de WAL-bestanden op.
            self._perform_housekeeping(ctx)

        return last_exit

    def _perform_housekeeping(self, ctx: ShellContext) -> None:
        """
        Forces a database checkpoint and closes connections to clean up WAL files.
        """
        try:
            db_mgr = getattr(ctx, 'db_manager', None)
            if db_mgr and hasattr(db_mgr, 'cleanup_all_wal_files'):
                self._log.debug("Starting automatic database housekeeping...")
                db_mgr.cleanup_all_wal_files()
        except Exception as e:
            self._log.error(f"Housekeeping failed: {e}")

    # --- Helper methods remain unchanged for brevity ---
    def _call_handler(self, handler, args, ctx, stdin):
        if handler is None: return 127
        sig = inspect.signature(handler)
        if len(sig.parameters) >= 3:
            return int(handler(args, ctx, stdin))
        return int(handler(args, ctx))

    def _run_external(self, name, args, stdin):
        try:
            proc = subprocess.run([name] + args, input=(stdin or ""), text=True, check=False)
            return int(proc.returncode)
        except FileNotFoundError:
            print(f"command not found: {name}")
            return 127

    def resolve_var(self, name: str, ctx: ShellContext) -> Optional[Any]:
        """
        Resolves a context variable, supporting direct variable names and
        dotted paths (e.g., 'project.id').
        """
        # 1. Check direct context variables
        if name in getattr(ctx, "_vars", {}):
            return ctx._vars[name]

        # 2. Check dotted paths starting with a context variable
        if "." in name:
            head, tail = name.split(".", 1)
            if head in getattr(ctx, "_vars", {}):
                # Resolve the path relative to the context variable's value
                return self._resolve_path(ctx._vars[head], tail)

        # 3. Check special root objects (like project) if they are attached to the context
        project_root = getattr(ctx, "project_manager", None)

        # Define possible root objects for dotted lookups
        roots = {
            # Assuming project manager holds the current project model/data,
            # or the context directly has a 'project' attribute.
            "project": project_root,
            "app": getattr(ctx, "context", None),  # Secondary app context reference
            "ctx": ctx,
        }

        # Check if the variable starts with a known root name
        head, *rest = name.split(".")
        if head in roots:
            # Resolve the path relative to the root object
            return self._resolve_path(roots[head], ".".join(rest))

        return None