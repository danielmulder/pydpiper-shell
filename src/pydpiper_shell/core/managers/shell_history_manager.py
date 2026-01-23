import logging
import shlex
import pandas as pd
import numpy as np
import os
import tempfile
import re
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Any

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.model import HistoryEntry
from pydpiper_shell.core.utils.path_utils import PathUtils

logger = logging.getLogger(__name__)

_COMMAND_LINE_PATTERN = re.compile(r"^\+(.*)$")
_TIMESTAMP_LINE_PATTERN = re.compile(
    r"^#\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\s*$"
)


class ShellHistoryManager:
    """
    Manages all operations related to shell command history, including parsing,
    optimization, and statistical analysis.
    """

    def __init__(self, ctx: ShellContext):
        self.ctx = ctx
        self.history_file = PathUtils.get_shell_history_file()
        self.backup_file = self.history_file.with_suffix('.bak')

    def display_info(self) -> int:
        """Calculates and displays key statistics about the history file."""
        all_entries = self._read_and_parse_history()
        if not all_entries:
            print("🤷 History is empty.")
            return 0

        # Calculate duplicates
        seen_commands = {entry.command for entry in all_entries}
        total_count = len(all_entries)
        unique_count = len(seen_commands)
        duplicate_count = total_count - unique_count

        # Get robust statistics
        threshold, median, mad = self._calc_outlier_threshold(
            [e.command for e in all_entries]
        )

        print("\n--- History Information & Stats ---")
        print(f"Total Commands Logged:   {total_count}")
        print(f"Unique Commands:         {unique_count}")
        print(f"Duplicate Commands:      {duplicate_count}")
        print("-" * 35)
        if unique_count > 5:
            print("Optimization Potential Report:")
            print(f"  - Median Command Length:  {median:.2f} tokens")
            print(f"  - Outlier Threshold:      {threshold} tokens (Median + 3*MAD)")
        print("-" * 35)
        return 0

    def _display_restart_disclaimer(self):
        """Displays a standard message advising the user to restart the shell."""
        print("\n⚠️ For changes to be fully reflected in history navigation (arrow keys),")
        print("   please restart the PydPiper shell.")

    def backup(self) -> int:
        """Creates a backup of the current history file."""
        if not self.history_file.exists():
            print("🤷 History file does not exist. Nothing to back up.")
            return 1
        try:
            shutil.copy2(self.history_file, self.backup_file)
            print(f"✅ History successfully backed up to: {self.backup_file}")
            return 0
        except Exception as e:
            print(f"❌ Error creating backup: {e}")
            return 1

    def rollback(self) -> int:
        """Restores the history from the backup file."""
        if not self.backup_file.exists():
            print("🤷 No backup file found. Cannot perform rollback.")
            return 1
        try:
            confirm = input(
                "Are you sure you want to overwrite the current history with the backup? [y/N]: "
            ).lower().strip()
            if confirm != 'y':
                print("Operation cancelled.")
                return 1

            shutil.copy2(self.backup_file, self.history_file)
            print("✅ History successfully restored from backup.")
            self._display_restart_disclaimer()
            return 0
        except Exception as e:
            print(f"❌ Error during rollback: {e}")
            return 1

    def reset(self, spec: Optional[str]) -> int:
        """Resets the history based on a specification string."""
        all_entries = self._read_and_parse_history()
        if not all_entries:
            print("🤷 History is already empty.")
            return 0

        original_count = len(all_entries)
        entries_to_keep = []
        action_description = ""

        if spec is None:
            action_description = "clear the entire command history"
            confirm = input(f"Are you sure you want to {action_description}? [y/N]: ").lower().strip()
            if confirm != 'y':
                print("Operation cancelled.")
                return 1
        else:
            try:
                if spec.startswith('-'):
                    count = int(spec[1:])
                    action_description = f"delete the last {count} command(s)"
                    entries_to_keep = all_entries[:-count] if count < original_count else []
                elif spec.startswith('+'):
                    count = int(spec[1:])
                    action_description = f"delete the first {count} command(s)"
                    entries_to_keep = all_entries[count:]
                else:
                    print(f"❌ Invalid format for --to: '{spec}'. Must start with '+' or '-'.")
                    return 1

                confirm = input(f"This will {action_description}. Are you sure? [y/N]: ").lower().strip()
                if confirm != 'y':
                    print("Operation cancelled.")
                    return 1
            except (ValueError, IndexError):
                print(f"❌ Invalid number in --to specification: '{spec}'.")
                return 1

        if not self._rewrite_history_atomically(entries_to_keep):
            print("❌ Critical error during history rewrite.")
            return 1

        removed_count = original_count - len(entries_to_keep)
        print(f"✅ History reset complete. Removed {removed_count} command(s).")
        print(f"   Total commands remaining: {len(entries_to_keep)}")
        self._display_restart_disclaimer()
        return 0

    def optimize(self, review_xl_input: Optional[Any], opt_potential_report: bool) -> int:
        """The main entry point for history optimization."""
        all_entries = self._read_and_parse_history()
        if not all_entries:
            print("🤷 History is empty. Nothing to optimize.")
            return 0

        seen_commands = set()
        deduplicated_entries_rev = []
        for entry in reversed(all_entries):
            if entry.command not in seen_commands:
                seen_commands.add(entry.command)
                deduplicated_entries_rev.append(entry)

        final_entries = list(reversed(deduplicated_entries_rev))
        duplicates_removed = len(all_entries) - len(final_entries)
        commands_for_processing = [entry.command for entry in final_entries]

        if opt_potential_report:
            self._report_statistics(commands_for_processing, duplicates_removed)
            print("\n✅ Report complete. History remains unchanged.")
            return 0

        xl_removed_count = 0
        if review_xl_input is not None:
            xl_threshold = self._determine_review_threshold(
                review_xl_input, commands_for_processing
            )
            if xl_threshold != float('inf'):
                commands_after_review = self._review_long_commands(
                    commands_for_processing, xl_threshold
                )
                xl_removed_count = len(commands_for_processing) - len(commands_after_review)
                if xl_removed_count > 0:
                    command_to_entry_map = {entry.command: entry for entry in final_entries}
                    final_entries = [command_to_entry_map[cmd] for cmd in commands_after_review]

        if duplicates_removed == 0 and xl_removed_count == 0:
            print("✅ History is already clean. No changes made.")
            return 0

        if not self._rewrite_history_atomically(final_entries):
            print("❌ Critical error during history rewrite. Changes may have been lost.")
            return 1

        print("\n--- History Optimization Summary ---")
        print(f"1. Duplicates removed: {duplicates_removed}")
        if review_xl_input is not None:
            print(f"2. Commands removed via --review-xl: {xl_removed_count}")
        print(f"Total commands remaining: {len(final_entries)}")
        print("------------------------------------")
        print("✅ Optimization complete. History order is preserved.")
        self._display_restart_disclaimer()
        return 0

    def _determine_review_threshold(self, review_xl_input: Any, commands: List[str]) -> float:
        """Determines the token count threshold for the interactive review."""
        if isinstance(review_xl_input, bool):
            if len(commands) < 10:
                print("⚠️ Not enough commands for a reliable dynamic threshold. Skipping XL-review.")
                return float('inf')
            else:
                threshold, _, _ = self._calc_outlier_threshold(commands)
                print(f"\nActivating XL-Review with Robust Dynamic Threshold: {threshold} tokens.")
                return float(threshold)
        else:
            return float(review_xl_input)

    @staticmethod
    def _get_token_count(cmd: str) -> int:
        """Helper to count tokens in a command string safely."""
        try:
            return len(shlex.split(cmd, posix=True))
        except ValueError:
            return len(cmd.split())

    def _calc_outlier_threshold(self, commands: List[str]) -> Tuple[int, float, float]:
        """Calculates a threshold to identify unusually long commands using MAD."""
        if not commands:
            return 0, 0.0, 0.0
        df = pd.DataFrame({'command': commands})
        df['token_count'] = df['command'].apply(self._get_token_count)
        median = df['token_count'].median()
        mad = (df['token_count'] - median).abs().median()

        # Calculate threshold (Median + 3 * MAD * consistency factor)
        if mad > 0:
            threshold = int(np.ceil(median + 3 * 1.4826 * mad + 2))
        else:
            threshold = int(np.ceil(median * 2.5))

        logger.info(
            f"review-xl threshold calculation: Median={median:.2f}, "
            f"MAD={mad:.2f}, Threshold={threshold}"
        )
        return threshold, median, mad

    def _report_statistics(self, commands: List[str], potential_duplicates: int):
        """Prints an optimization potential report."""
        print("\n--- Optimization Potential Report (Robust) ---")
        print(f"Total Unique Commands: {len(commands)}")
        print(f"Potential Duplicates Found: {potential_duplicates}")
        print("-" * 55)
        if len(commands) < 5:
            print("🤷 Not enough unique commands for meaningful statistics.")
            return
        threshold, median, mad = self._calc_outlier_threshold(commands)
        print(f"📊 MEDIAN LENGTH: {median:.2f} tokens")
        print(f"📈 MEDIAN ABSOLUTE DEVIATION (MAD): {mad:.2f} tokens")
        print("-" * 55)
        print(f"🎯 ROBUST THRESHOLD (Median + 3*MAD*k): {threshold} tokens")
        print("   (This threshold is highly resistant to outliers)")
        print("-" * 55)

    def _review_long_commands(self, history: List[str], threshold: int) -> List[str]:
        """Interactive review process for commands exceeding the threshold."""
        print(f"\n🚀 Starting --review-xl review (Threshold: {threshold} tokens)...")
        commands_to_keep = []
        removed_count = 0
        for i, cmd in enumerate(history):
            if self._get_token_count(cmd) > threshold:
                print("-" * 50)
                print(f"[{i + 1}/{len(history)}] ⚠️ Detected LONG command ({self._get_token_count(cmd)} tokens):")
                print(f"   -> {cmd}")
                try:
                    if input("Press ENTER to keep, 'd' to delete: ").strip().lower() == 'd':
                        print("   -> Command DELETED.")
                        removed_count += 1
                    else:
                        commands_to_keep.append(cmd)
                        print("   -> Command KEPT.")
                except (EOFError, KeyboardInterrupt):
                    print("\nReview cancelled. Keeping remaining commands.")
                    commands_to_keep.extend(history[i:])
                    break
            else:
                commands_to_keep.append(cmd)
        print("-" * 50)
        print(f"✨ XL-review complete. Removed {removed_count} command(s).")
        return commands_to_keep

    def _read_and_parse_history(self) -> List[HistoryEntry]:
        """Reads the history file and parses it into HistoryEntry objects."""
        if not self.history_file.exists():
            return []
        try:
            content = self.history_file.read_text(encoding='utf-8').splitlines()
        except Exception as e:
            logger.error(f"Failed to read history file: {e}")
            return []

        entries, current_lines, last_ts = [], [], datetime.now(timezone.utc)

        def commit():
            nonlocal current_lines, last_ts
            if current_lines:
                entries.append(HistoryEntry(command="\n".join(current_lines), timestamp=last_ts))
                current_lines = []

        for line in content:
            ts_match = _TIMESTAMP_LINE_PATTERN.match(line.strip())
            cmd_match = _COMMAND_LINE_PATTERN.match(line)
            if ts_match:
                commit()
                try:
                    ts_text = ts_match.group(1)
                    if ts_text.endswith('Z'):
                        ts_text = ts_text[:-1] + '+00:00'
                    last_ts = datetime.fromisoformat(ts_text)
                except ValueError:
                    last_ts = datetime.now(timezone.utc)
            elif cmd_match:
                current_lines.append(cmd_match.group(1))
            else:
                commit()
        commit()
        return entries

    def _rewrite_history_atomically(self, final_entries: List[HistoryEntry]) -> bool:
        """Writes the new history to a temp file and replaces the old one atomically."""
        temp_path = None

        try:
            # Generate the content
            content = "".join(
                f"{''.join([f'+{line}' for line in e.command.splitlines(True)]) or f'+{e.command}'}"
                f"\n# {e.timestamp.isoformat()}\n\n"
                for e in final_entries
            )

            # Create the temporary file
            fd, temp_path_str = tempfile.mkstemp(
                dir=self.history_file.parent,
                prefix=f"{self.history_file.name}.",
                suffix=".tmp"
            )

            temp_path = Path(temp_path_str)

            # Write content to the temporary file
            with os.fdopen(fd, "w", encoding="utf-8", newline='\n') as f:
                f.write(content)

            # Replace the old file atomically
            os.replace(temp_path, self.history_file)
            return True

        except Exception as e:
            logger.error(f"Critical: Failed to rewrite history: {e}", exc_info=True)

            # Safe cleanup of the temp file
            if temp_path is not None and temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass

            return False