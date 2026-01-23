# tests/core/test_config_management.py
import pytest
import json
from pathlib import Path

from pydpiper_shell.core.managers.config_manager import ConfigManager
from pydpiper_shell.core.handlers.config_handler import handle_config
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.utils.path_utils import PathUtils

# Een standaard, voorspelbare configuratie voor onze tests
MOCK_SETTINGS_CONTENT = {
    "debug": {
        "level": "WARNING"
    },
    "crawler": {
        "default_max_pages": 5000,
        "flush_interval": 10
    }
}


@pytest.fixture
def config_env(tmp_path, monkeypatch):
    """
    Een fixture die een geïsoleerde testomgeving opzet voor de ConfigManager:
    - Creëert een tijdelijke package root.
    - Plaatst daarin een nep 'settings.json' bestand.
    - Monkeypatched PathUtils om naar deze tijdelijke locatie te wijzen.
    """
    package_root = tmp_path / "pydpiper_shell"
    package_root.mkdir()
    settings_file = package_root / "settings.json"
    settings_file.write_text(json.dumps(MOCK_SETTINGS_CONTENT))

    # Zorg ervoor dat de ConfigManager ons testbestand vindt
    monkeypatch.setattr(PathUtils, 'get_shell_package_root', lambda: package_root)

    # We moeten een nieuwe instantie van de singleton maken voor de test
    # omdat de globale instantie al geladen kan zijn.
    config_manager_instance = ConfigManager()
    config_manager_instance.reset()  # Forceer herladen vanuit ons nep-bestand

    return config_manager_instance, ShellContext()


# --- Tests voor de ConfigManager direct ---

def test_config_manager_load(config_env):
    """Test of de manager de configuratie correct laadt."""
    manager, _ = config_env
    config = manager.get_all()
    assert config["debug"]["level"] == "WARNING"
    assert config["crawler"]["default_max_pages"] == 5000


def test_config_manager_get_nested(config_env):
    """Test het ophalen van geneste waarden."""
    manager, _ = config_env
    assert manager.get_nested("crawler.flush_interval") == 10
    assert manager.get_nested("non.existent.key", "default") == "default"


def test_config_manager_set_nested(config_env):
    """Test het aanpassen van waarden in het geheugen."""
    manager, _ = config_env

    # Test het aanpassen van een bestaande waarde
    manager.set_nested("debug.level", "INFO")
    assert manager.get_nested("debug.level") == "INFO"

    # Test het toevoegen van een nieuwe sleutel
    manager.set_nested("new_feature.enabled", "True")
    assert manager.get_nested("new_feature.enabled")

    # Test type-casting: de originele waarde is een int, dus de nieuwe string '20'
    # moet worden omgezet naar een int.
    manager.set_nested("crawler.flush_interval", "20")
    assert manager.get_nested("crawler.flush_interval") == 20
    assert isinstance(manager.get_nested("crawler.flush_interval"), int)


def test_config_manager_reset(config_env):
    """Test of de reset-functie de configuratie herlaadt vanaf schijf."""
    manager, _ = config_env

    # Verander eerst een waarde in het geheugen
    manager.set_nested("debug.level", "DEBUG")
    assert manager.get_nested("debug.level") == "DEBUG"

    # Voer de reset uit
    manager.reset()

    # Controleer of de waarde is teruggezet naar de originele waarde uit het bestand
    assert manager.get_nested("debug.level") == "WARNING"


# --- Tests voor de 'config' command handler ---

def test_handle_config_list(config_env, capsys):
    """Test 'config list'."""
    _, ctx = config_env
    handle_config(["list"], ctx)
    captured = capsys.readouterr()

    output_json = json.loads(captured.out)
    assert output_json["crawler"]["default_max_pages"] == 5000


def test_handle_config_set(config_env, capsys):
    """Test 'config set <key> <value>'."""
    _, ctx = config_env
    handle_config(["set", "crawler.default_max_pages", "1234"], ctx)
    captured = capsys.readouterr()

    assert "Config updated: crawler.default_max_pages = 1234" in captured.out

    # Controleer of de manager de wijziging daadwerkelijk heeft doorgevoerd
    manager = config_env[0]
    assert manager.get_nested("crawler.default_max_pages") == 1234


def test_handle_config_reset(config_env, capsys):
    """Test 'config reset'."""
    manager, ctx = config_env

    # Verander eerst een waarde
    handle_config(["set", "debug.level", "CRITICAL"], ctx)
    assert manager.get_nested("debug.level") == "CRITICAL"

    # Roep dan reset aan
    handle_config(["reset"], ctx)
    captured = capsys.readouterr()

    assert "Configuration has been reset" in captured.out
    # Controleer of de waarde is teruggezet naar het origineel
    assert manager.get_nested("debug.level") == "WARNING"