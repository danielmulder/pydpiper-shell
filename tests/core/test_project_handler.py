import pytest
from unittest.mock import MagicMock, patch
from pydpiper_shell.core.handlers.project_handler import handle_project
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.model import Project


@pytest.fixture
def mock_context():
    ctx = ShellContext()
    # We mocken de manager op de context
    ctx.project_manager = MagicMock()
    ctx.active_project = None
    return ctx


def test_project_create_success(mock_context, capsys):
    test_url = "https://example.com"
    mock_project = Project(id=1, name="example.com", start_url=test_url)
    mock_context.project_manager.create_project.return_value = mock_project

    args = ["create", test_url]
    exit_code = handle_project(args, mock_context)

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Project 'example.com' created" in captured.out
