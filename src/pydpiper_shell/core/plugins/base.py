# src/pydpiper_shell/core/plugins/core.py
import abc
from .facade import PluginFacade

class PluginBase(metaclass=abc.ABCMeta):
    """
    Abstract base class for all plugins.

    Every plugin must inherit from this class and implement the 'run' method.
    """
    @abc.abstractmethod
    def run(self, app: PluginFacade, args: list[str]) -> int:
        """
        The main entrypoint for the plugin execution.

        Args:
            app: The PluginFacade providing access to shell resources (context, managers).
            args: A list of extra arguments passed to the plugin from the
                  command line (e.g., "plugin run my_plugin arg1 arg2").

        Returns:
            An integer exit code (0 for success, non-zero for error).
        """
        raise NotImplementedError("Every plugin must implement a 'run' method.")