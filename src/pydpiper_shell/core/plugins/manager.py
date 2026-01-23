# src/pydpiper_shell/core/plugins/manager.py
import importlib.util
import logging
from pathlib import Path
from typing import List, Union, Dict, Any

from pydpiper_shell.core.context.shell_context import ShellContext
# PathUtils is imported to determine the path to the plugins.
from pydpiper_shell.core.utils.path_utils import PathUtils
from .base import PluginBase
from .facade import PluginFacade

logger = logging.getLogger(__name__)


class PluginManager:
    """Discovers, loads, and manages the execution of plugins."""

    def __init__(self, plugin_dir: Union[str, Path, None] = None):
        """
        Initializes the manager.

        Uses a reliable path via PathUtils instead of the current working directory.
        """
        # The manager now uses PathUtils for a reliable path.
        # Path.cwd() is replaced for consistency.
        self.plugin_dir = Path(plugin_dir) if plugin_dir else PathUtils.get_plugins_dir()
        self.plugin_dir.mkdir(exist_ok=True)
        logger.debug("Plugin directory is set to: %s", self.plugin_dir)

    def discover_plugins(self) -> List[str]:
        """
        Searches for valid plugin files (*_plugin.py) in the plugin directory and all subdirectories.

        Returns:
            List[str]: A sorted list of discovered plugin base names (without the '_plugin.py' suffix).
        """
        plugins = []
        # This glob pattern searches recursively into all subdirectories (like 'modules').
        for file_path in self.plugin_dir.glob("**/*_plugin.py"):
            plugins.append(file_path.stem.removesuffix("_plugin"))  # Use the base name without the suffix
        return sorted(plugins)

    def run_plugin(self, plugin_name: str, args: List[str], ctx: ShellContext) -> int:
        """
        Loads and executes a specific plugin.

        Args:
            plugin_name (str): The base name of the plugin (e.g., 'sitemap_generator').
            args (List[str]): Arguments to pass to the plugin's run method.
            ctx (ShellContext): The current shell context.

        Returns:
            int: The exit code returned by the plugin's run method.
        """

        # Ensure the filename ends with the required suffix for globbing
        plugin_name_base = plugin_name if plugin_name.endswith("_plugin") else f"{plugin_name}_plugin"

        # Recursively search for the plugin file
        plugin_files = list(self.plugin_dir.glob(f"**/{plugin_name_base}.py"))

        if not plugin_files:
            print(f"Error: Plugin '{plugin_name}' not found anywhere in {self.plugin_dir}")
            return 1

        plugin_file = plugin_files[0]

        try:
            # Dynamically import the module from the file path
            spec = importlib.util.spec_from_file_location(plugin_name_base, plugin_file)
            if not spec or not spec.loader:
                raise ImportError("Could not create a module spec for %s", plugin_file)

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            plugin_class = None
            # Find the class that inherits from PluginBase
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                # Ensure it is a class, a subclass of PluginBase, and not PluginBase itself
                if isinstance(attr, type) and issubclass(attr, PluginBase) and attr is not PluginBase:
                    plugin_class = attr
                    break

            if not plugin_class:
                raise TypeError("Plugin file must contain a class inheriting from PluginBase.")

            # Initialize and run the plugin
            instance = plugin_class()

            # --- FIX: Maak de facade aan, zelfs zonder actief project ---
            # Haal project_id op als het bestaat, anders 0 (als default/placeholder)
            project_id_str = ctx.get("project.id")
            project_id = int(project_id_str) if project_id_str and project_id_str.isdigit() else 0
            facade = PluginFacade(project_id, ctx)

            print(f"Running plugin: {plugin_name}...")
            exit_code = instance.run(facade, args)
            print(f"✅ Plugin '{plugin_name}' finished with exit code {exit_code}.")
            return exit_code

        except Exception as e:
            logger.error("Failed to run plugin '%s': %s", plugin_name, e, exc_info=True)
            print(f"❌ Error running plugin '{plugin_name}': {e}")
            return 1