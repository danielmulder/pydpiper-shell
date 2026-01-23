import logging
import re
from typing import Dict, Any, Iterable

from prompt_toolkit.completion import Completion
from prompt_toolkit.document import Document
from prompt_toolkit.history import History

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.config_manager import config_manager
from pydpiper_shell.core.managers.workflow_manager import WorkflowManager

logger = logging.getLogger(__name__)

OPERATORS = {"&&", "||", ";", "|"}
# Regex to find the last operator *before* the cursor
OPERATOR_PATTERN = re.compile(r"(\s+(?:&&|\|\||;|\|)\s+)")


class CompletionManager:
    """
    Manages logic for generating command completion suggestions, handling operators correctly.
    """

    def __init__(
        self,
        shell_context: ShellContext,
        history: History,
        command_hierarchy: Dict[str, Any]
    ):
        self.ctx = shell_context
        self.history = history
        self.command_hierarchy = command_hierarchy
        self.workflow_manager = WorkflowManager(shell_context.db_mgr)

    def generate_completions(self, document: Document) -> Iterable[Completion]:
        """
        Generates completions based on the text segment after the last operator
        before the cursor.
        """
        text_before_cursor = document.text_before_cursor

        # --- Special Triggers (!c, !h, !w) ---
        # These override normal completion if they are at the end
        if text_before_cursor.endswith('!c'):
            yield from self._get_main_command_completions('!c')
            return
        if text_before_cursor.endswith('!h'):
            yield from self._get_history_completions()
            return
        if text_before_cursor.endswith('!w'):
            yield from self._get_workflow_completions()
            return

        last_op_match = None
        for match in OPERATOR_PATTERN.finditer(text_before_cursor):
            last_op_match = match

        segment_start_index = 0
        if last_op_match:
            # Start analysis after the operator and surrounding spaces
            segment_start_index = last_op_match.end()

        relevant_text = text_before_cursor[segment_start_index:]
        words_in_segment = relevant_text.lstrip().split()
        word_before_cursor = document.get_word_before_cursor(WORD=True)

        if "@{" in relevant_text and (word_before_cursor.startswith("@{") or document.char_before_cursor == '{'):
            yield from self._get_variable_completions(word_before_cursor)
            return

        # --- Command/Subcommand Suggestions based on segment ---

        num_words_in_segment = len(words_in_segment)
        # Determine if we are completing the first word (main command) or second (subcommand)
        # within the current segment.
        is_completing_first_word = (
            num_words_in_segment == 0 or
            (num_words_in_segment == 1 and not relevant_text.endswith(" "))
        )
        is_completing_second_word = (
            (num_words_in_segment == 1 and relevant_text.endswith(" ")) or
            (num_words_in_segment == 2 and not relevant_text.endswith(" "))
        )

        # Suggest main commands if we are completing the first word of the segment
        if is_completing_first_word:
            yield from self._get_main_command_completions(word_before_cursor)

        # Suggest subcommands if we have a main command and are completing the second word
        elif is_completing_second_word and num_words_in_segment > 0:
            main_command_in_segment = words_in_segment[0]
            if main_command_in_segment in self.command_hierarchy:
                hierarchy_entry = self.command_hierarchy.get(main_command_in_segment)
                if isinstance(hierarchy_entry, dict):  # Check if it HAS subcommands
                    subcommands = hierarchy_entry.keys()
                    # Determine the part of the subcommand already typed
                    if num_words_in_segment == 2 and not relevant_text.endswith(" "):
                        sub_word_to_complete = words_in_segment[1]
                    else:
                        sub_word_to_complete = ""
                    yield from self._get_sub_command_completions(subcommands, sub_word_to_complete)

    # --- Helper methods for different completion types ---

    def _get_main_command_completions(self, word_before_cursor: str) -> Iterable[Completion]:
        """Yields main command completions."""
        is_trigger = word_before_cursor == '!c'
        start_pos = -2 if is_trigger else -len(word_before_cursor)
        for command_name in sorted(self.command_hierarchy.keys()):
            # If not triggered by !c, filter based on the typed word
            if is_trigger or command_name.startswith(word_before_cursor):
                yield Completion(
                    command_name,
                    start_position=start_pos,
                    display_meta="Main Command"
                )

    def _get_history_completions(self) -> Iterable[Completion]:
        """Yields command history completions."""
        max_len = config_manager.get_nested("autocomplete.h_max_len", 5)
        logger.debug(f"History completion (!h) triggered. Max items: {max_len}")
        recent_commands, seen = [], set()
        for command in reversed(self.history.get_strings()):
            command_stripped = command.strip()
            if command_stripped and command_stripped != '!h' and command_stripped not in seen:
                seen.add(command_stripped)
                recent_commands.append(command_stripped)
                if len(recent_commands) >= max_len:
                    break
        for command in recent_commands:
            yield Completion(command, start_position=-2, display_meta="Command History")

    def _get_workflow_completions(self) -> Iterable[Completion]:
        """Yields workflow run command completions."""
        logger.debug("Workflow completion (!w) triggered.")
        workflows = self.workflow_manager.load_all()
        if workflows:
            for wf in sorted(workflows, key=lambda w: w.name):
                suggestion = f"workflow {wf.name}"
                yield Completion(
                    suggestion, start_position=-2,
                    display_meta=f"Workflow: {wf.description or 'No description'}"
                )
        else:
            yield Completion("!w", start_position=-2, display_meta="No workflows found")

    def _get_variable_completions(self, word_before_cursor: str) -> Iterable[Completion]:
        """Yields context variable completions."""
        prefix = word_before_cursor if word_before_cursor.startswith("@{") else ""
        start_pos = -len(prefix)
        variables = self.ctx._vars.keys()
        for var_name in sorted(variables):
            suggestion = f"@{{{var_name}}}"
            if suggestion.startswith(prefix):
                yield Completion(suggestion, start_position=start_pos, display_meta="Context Variable")

    def _get_sub_command_completions(self, subcommands: Iterable[str], word_before_cursor: str) -> Iterable[Completion]:
        """Yields subcommand completions."""
        start_pos = -len(word_before_cursor)
        for sub in sorted(subcommands):
            if sub.startswith(word_before_cursor):
                yield Completion(sub, start_position=start_pos)