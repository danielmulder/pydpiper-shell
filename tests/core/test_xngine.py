# tests/core/test_xngine.py
import pytest
import re
from unittest.mock import MagicMock

# De te testen componenten
from pydpiper_shell.core.xngine import ExecuteEngine
from pydpiper_shell.core.context.shell_context import ShellContext


# Een paar simpele, nep-handlers voor onze tests
def mock_handler_success(args, ctx, stdin=None):
    ctx.set("last_called", "success")
    return 0  # Succes


def mock_handler_failure(args, ctx, stdin=None):
    ctx.set("last_called", "failure")
    return 1  # Fout


def mock_handler_pipe(args, ctx, stdin=None):
    # Deze handler simuleert het ontvangen van data via een pipe
    ctx.set("pipe_input", stdin)
    return 0


@pytest.fixture
def shell_context():
    """Een fixture die een schone ShellContext voor elke test levert."""
    return ShellContext()


@pytest.fixture
def xngine():
    """Een fixture die een geconfigureerde ExecuteEngine levert."""
    # Mock de functies die de engine normaal gebruikt
    mock_registry = {
        "cmd_ok": mock_handler_success,
        "cmd_fail": mock_handler_failure,
        "cmd_pipe": mock_handler_pipe
    }

    # Gebruik een simpele lambda die geen argumenten expandeert
    mock_expander = lambda name, args, ctx: args

    engine = ExecuteEngine(
        command_registry=mock_registry,
        var_pattern=re.compile(r"@\{([^}]+)\}"),
        maybe_expand_args=mock_expander,
        post_refresh=lambda ctx: None,  # Geen actie na commando
        logger=MagicMock()  # Gebruik MagicMock direct
    )
    return engine


def test_xngine_execute_simple_success(xngine, shell_context):
    """Test de uitvoering van één succesvol commando."""
    commands = [("cmd_ok", [], None)]
    exit_code = xngine.execute_sequence(commands, shell_context)

    assert exit_code == 0
    assert shell_context.get("last_called") == "success"


def test_xngine_operator_and_success(xngine, shell_context):
    """Test de '&&' operator: tweede commando moet draaien na succes."""
    commands = [("cmd_ok", [], None), ("cmd_ok", [], "&&")]
    exit_code = xngine.execute_sequence(commands, shell_context)

    assert exit_code == 0
    # De context moet zijn bijgewerkt door de tweede (en laatste) aanroep
    assert shell_context.get("last_called") == "success"


def test_xngine_operator_and_failure(xngine, shell_context):
    """Test de '&&' operator: tweede commando mag niet draaien na een fout."""
    commands = [("cmd_fail", [], None), ("cmd_ok", [], "&&")]
    exit_code = xngine.execute_sequence(commands, shell_context)

    assert exit_code == 1
    # De context moet zijn bijgewerkt door de eerste aanroep, de tweede is nooit bereikt
    assert shell_context.get("last_called") == "failure"


def test_xngine_operator_or_success(xngine, shell_context):
    """Test de '||' operator: tweede commando mag niet draaien na succes."""
    commands = [("cmd_ok", [], None), ("cmd_fail", [], "||")]
    exit_code = xngine.execute_sequence(commands, shell_context)

    assert exit_code == 0
    assert shell_context.get("last_called") == "success"


def test_xngine_operator_or_failure(xngine, shell_context):
    """Test de '||' operator: tweede commando moet draaien na een fout."""
    commands = [("cmd_fail", [], None), ("cmd_ok", [], "||")]
    exit_code = xngine.execute_sequence(commands, shell_context)

    assert exit_code == 0
    assert shell_context.get("last_called") == "success"


def test_xngine_operator_pipe(xngine, shell_context, capsys):
    """Test de '|' operator. We mocken 'echo' door een handler te maken die output produceert."""

    # We voegen een tijdelijke 'echo' handler toe aan de engine voor deze test
    def mock_echo(args, ctx, stdin=None):
        print("hallo wereld")
        return 0

    xngine._commands["echo"] = mock_echo

    commands = [("echo", [], None), ("cmd_pipe", [], "|")]
    exit_code = xngine.execute_sequence(commands, shell_context)

    assert exit_code == 0
    # Controleer of de cmd_pipe handler de output van echo heeft ontvangen
    assert shell_context.get("pipe_input") == "hallo wereld\n"


def test_xngine_variable_expansion(xngine, shell_context):
    """Test of de engine variabelen correct expandeert."""
    shell_context.set("project.id", "42")

    # We passen de mock expander aan voor deze test
    xngine._maybe_expand_args = lambda name, args, ctx: [xngine.expand_context_vars(a, ctx) for a in args]

    # Mock een handler die de ontvangen args opslaat
    received_args = []

    def arg_catcher(args, ctx, stdin=None):
        nonlocal received_args
        received_args = args
        return 0

    xngine._commands["catch"] = arg_catcher

    commands = [("catch", ["project-id-is-@{project.id}"], None)]
    xngine.execute_sequence(commands, shell_context)

    assert received_args == ["project-id-is-42"]