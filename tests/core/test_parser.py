# tests/core/test_parse_images.py
import pytest
from pydpiper_shell.core.parser import parse_command_line

def test_parse_simple_command():
    """Test een enkelvoudig commando zonder argumenten."""
    result = parse_command_line("project list")
    assert result == [("project", ["list"], None)]

def test_parse_command_with_arguments():
    """Test een commando met meerdere argumenten."""
    result = parse_command_line("project create https://example.com --mode discovery")
    assert result == [("project", ["create", "https://example.com", "--mode", "discovery"], None)]

def test_parse_sequential_operator():
    """Test de ';' operator voor sequentiële uitvoering."""
    result = parse_command_line("project list ; context vars")
    assert result == [
        ("project", ["list"], None),
        ("context", ["vars"], ";")
    ]

def test_parse_conditional_and_operator():
    """Test de '&&' operator voor conditioneel succes."""
    result = parse_command_line("project load 1 && crawler status")
    assert result == [
        ("project", ["load", "1"], None),
        ("crawler", ["status"], "&&")
    ]

def test_parse_conditional_or_operator():
    """Test de '||' operator voor conditionele mislukking."""
    result = parse_command_line("project load 999 || project list")
    assert result == [
        ("project", ["load", "999"], None),
        ("project", ["list"], "||")
    ]

def test_parse_pipe_operator():
    """Test de '|' operator voor het doorgeven van output."""
    result = parse_command_line("echo @{project.name} | plugin run word_count")
    assert result == [
        ("echo", ["@{project.name}"], None),
        ("plugin", ["run", "word_count"], "|")
    ]

def test_parse_complex_chain():
    """Test een complexe keten met meerdere operatoren."""
    line = "project load 1 && echo 'Loaded' || echo 'Failed' ; sys ram_info"
    result = parse_command_line(line)
    assert result == [
        ("project", ["load", "1"], None),
        ("echo", ["Loaded"], "&&"),
        ("echo", ["Failed"], "||"),
        ("sys", ["ram_info"], ";")
    ]

def test_parse_variable_shorthands():
    """Test de shorthands voor 'set' en 'get'."""
    # Test 'set' shorthand
    result_set = parse_command_line("@{my_var}=mijn_waarde")
    assert result_set == [("set", ["@{my_var}=mijn_waarde"], None)]

    # Test 'get' shorthand
    result_get = parse_command_line("@{my_var}")
    assert result_get == [("get", ["@{my_var}"], None)]

def test_parse_quoted_arguments():
    """Test argumenten met aanhalingstekens om spaties te behouden."""
    line = 'workflow create "project create @{url} && crawler run" --name "safe_start"'
    result = parse_command_line(line)
    assert result == [
        ("workflow", ["create", "project create @{url} && crawler run", "--name", "safe_start"], None)
    ]

def test_parse_empty_and_whitespace_input():
    """Test of lege invoer correct wordt afgehandeld."""
    assert parse_command_line("") == []
    assert parse_command_line("    ") == []