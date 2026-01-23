import pytest
from unittest.mock import patch, MagicMock
from pydpiper_shell.core.handlers.crawler_handler import handle_crawler
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.model import Project

@pytest.fixture
def mock_context():
    ctx = ShellContext()
    ctx.project_manager = MagicMock()
    ctx.cache_mgr = MagicMock()
    return ctx

@patch('pydpiper_shell.core.handlers.crawler_handler.run_on_main_loop')
@patch('pydpiper_shell.core.handlers.crawler_handler.AsyncCrawlController')
def test_crawler_run_success(mock_controller_class, mock_loop_runner, mock_context, capsys):
    test_project_id = 1
    test_project = Project(id=test_project_id, name="test.com", start_url="https://test.com")
    mock_context.project_manager.get_project_by_id.return_value = test_project

    mock_controller_instance = MagicMock()
    mock_coro = "MOCKED_CORO"
    mock_controller_instance.run.return_value = mock_coro
    mock_controller_class.return_value = mock_controller_instance
    mock_loop_runner.return_value = 0

    args = ["run", "--project", "1"]
    exit_code = handle_crawler(args, mock_context)

    assert exit_code == 0
    mock_context.project_manager.get_project_by_id.assert_called_once_with(test_project_id)
    #mock_loop_runner.assert_called_once_with(mock_coro)