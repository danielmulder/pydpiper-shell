# tests/test_history_management.py
import pytest
from pathlib import Path

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.shell_history_manager import ShellHistoryManager
from pydpiper_shell.core.utils.path_utils import PathUtils
from prompt_toolkit.history import FileHistory

# Generate a predictable history for the tests: 10 commands, with 'command1' as a duplicate.
HISTORY_COMMANDS = [f"command{i}" for i in range(1, 10)] + ["command1"]
HISTORY_CONTENT = ""
for i, cmd in enumerate(HISTORY_COMMANDS):
    # We use a simple format that our parser can handle.
    HISTORY_CONTENT += f"+{cmd}\n# 2025-01-01T12:00:0{i}\n\n"


@pytest.fixture
def history_env(tmp_path, monkeypatch):
    """
    A pytest fixture that sets up an isolated test environment:
    - A temporary directory.
    - A mock history file with predictable content.
    - A ShellContext pointing to this mock file.
    """
    history_file = tmp_path / ".test_history"
    history_file.write_text(HISTORY_CONTENT)

    # Ensure the manager uses our test file, not the real one.
    monkeypatch.setattr(PathUtils, 'get_shell_history_file', lambda: history_file)

    # Create a context and a mock prompt_session.
    ctx = ShellContext()

    class MockPromptSession:
        def __init__(self, path):
            self.history = FileHistory(str(path))

    ctx.prompt_session = MockPromptSession(history_file)

    return ctx, history_file


# --- Tests ---

def test_info_command(history_env, capsys):
    """Test if the 'history info' command displays the correct statistics."""
    ctx, _ = history_env
    manager = ShellHistoryManager(ctx)

    manager.display_info()
    captured = capsys.readouterr()

    assert "Total Commands Logged:   10" in captured.out
    assert "Unique Commands:         9" in captured.out
    assert "Duplicate Commands:      1" in captured.out
    assert "Median Command Length:" in captured.out


def test_backup_and_rollback(history_env, monkeypatch):
    """Test if the backup and rollback functionality works correctly."""
    ctx, history_file = history_env
    manager = ShellHistoryManager(ctx)

    # 1. Create a backup.
    assert manager.backup() == 0
    backup_file = history_file.with_suffix('.bak')
    assert backup_file.exists()
    original_content = history_file.read_text()
    assert backup_file.read_text() == original_content

    # 2. Corrupt the original file.
    history_file.write_text("CORRUPTED DATA")
    assert history_file.read_text() != original_content

    # 3. Perform a rollback (simulate 'y' for confirmation).
    monkeypatch.setattr('builtins.input', lambda _: 'y')
    assert manager.rollback() == 0

    # 4. Verify that the file has been restored.
    assert history_file.read_text() == original_content


def test_reset_all_clears_history(history_env, monkeypatch):
    """Test if 'history reset' clears the entire file."""
    ctx, history_file = history_env
    manager = ShellHistoryManager(ctx)

    monkeypatch.setattr('builtins.input', lambda _: 'y')
    assert manager.reset(spec=None) == 0

    assert history_file.read_text().strip() == ""


def test_reset_to_minus_deletes_last_entries(history_env, monkeypatch):
    """Test if 'reset --to -N' correctly deletes the last N commands."""
    ctx, history_file = history_env
    manager = ShellHistoryManager(ctx)

    monkeypatch.setattr('builtins.input', lambda _: 'y')
    # We have 10 commands; we will delete the last 3.
    assert manager.reset(spec="-3") == 0

    entries = manager._read_and_parse_history()
    commands = [e.command for e in entries]

    assert len(commands) == 7
    assert "command7" in commands
    assert "command8" not in commands
    assert "command9" not in commands
    # The last "command1" (the duplicate) should be removed.
    assert "command1" in commands


def test_reset_to_plus_deletes_first_entries(history_env, monkeypatch):
    """Test if 'reset --to +N' correctly deletes the first N commands."""
    ctx, history_file = history_env
    manager = ShellHistoryManager(ctx)

    monkeypatch.setattr('builtins.input', lambda _: 'y')
    # We will delete the first 4 commands.
    assert manager.reset(spec="+4") == 0

    entries = manager._read_and_parse_history()
    commands = [e.command for e in entries]

    assert len(commands) == 6
    # The first 'command1' is gone, but the duplicate at the end remains.
    assert "command1" in commands
    assert "command2" not in commands
    assert "command3" not in commands
    assert "command4" not in commands
    assert "command5" in commands


def test_optimize_deduplicates_history(history_env, monkeypatch):
    """Test if 'opt history' correctly removes duplicate commands."""
    ctx, history_file = history_env
    manager = ShellHistoryManager(ctx)

    # We don't want the disclaimer in the test output.
    monkeypatch.setattr(manager, '_display_restart_disclaimer', lambda: None)

    # Call optimize without any flags (default deduplication).
    assert manager.optimize(review_xl_input=None, opt_potential_report=False) == 0

    entries = manager._read_and_parse_history()
    # There should be one less command now.
    assert len(entries) == 9

    # Verify that the older 'command1' was removed and the newest one was kept.
    commands = [e.command for e in entries]
    assert commands.count("command1") == 1
    assert commands[0] == "command2"  # The first 'command1' is gone.