# src/auditor/dom/registry.py
import importlib
import pkgutil
import logging
from typing import Dict, List, Callable, Any, Optional, Set

from .core import ElementDefinition

logger = logging.getLogger(__name__)


class DOMRegistry:
    """
    Central registry for DOM elements, parsers, and audit rules.

    Dynamically discovers and loads ElementDefinition modules from the
    'auditor.dom.elements' package to populate parsers, rules, and issue codes.
    """

    _parsers: Dict[str, Callable] = {}
    _audit_rules: List[Callable] = []
    _all_codes: Set[str] = set()
    _loaded: bool = False

    @classmethod
    def discover(cls) -> None:
        """
        Discovers and registers all element definitions found in the 'auditor.dom.elements' package.

        This method scans the `auditor.dom.elements` package for modules containing a
        `DEFINITION` attribute (instance of `ElementDefinition`). It registers parsers,
        audit rules, and collects all possible issue codes.
        """
        if cls._loaded:
            return

        try:
            # Import the elements package to iterate over its modules
            import auditor.dom.elements as elements_pkg

            for _, name, _ in pkgutil.iter_modules(elements_pkg.__path__):
                full_name = f"auditor.dom.elements.{name}"
                try:
                    module = importlib.import_module(full_name)
                    if hasattr(module, "DEFINITION") and isinstance(module.DEFINITION, ElementDefinition):
                        defn = module.DEFINITION

                        # Register parser for the specific tag
                        cls._parsers[defn.tag_name] = defn.parser

                        # Register all audit rules associated with this definition
                        for rule in defn.audit_rules:
                            cls._register_rule(defn.model, rule)

                        # Collect possible issue codes if defined
                        if hasattr(defn, "codes"):
                            cls._all_codes.update(defn.codes)

                        logger.debug(f"EVP loaded: {defn.tag_name}")
                except Exception as e:
                    logger.error(f"Error loading module {name}: {e}")

            cls._loaded = True
        except ImportError as e:
            logger.error(f"Could not find elements package: {e}")

    @classmethod
    def _register_rule(cls, model_type: Any, rule_func: Callable) -> None:
        """
        Registers a single audit rule, wrapping it with a type check.

        Args:
            model_type: The class type this rule applies to.
            rule_func: The function executing the logic.
        """
        def wrapped(node: Any) -> Any:
            if isinstance(node, model_type):
                return rule_func(node)
            return ""

        cls._audit_rules.append(wrapped)

    @classmethod
    def get_parser(cls, tag_name: str) -> Optional[Callable]:
        """Retrieves the parser function for a specific HTML tag."""
        return cls._parsers.get(tag_name)

    @classmethod
    def get_all_rules(cls) -> List[Callable]:
        """Returns a list of all registered audit rule functions."""
        return cls._audit_rules

    @classmethod
    def get_all_possible_codes(cls) -> List[str]:
        """
        Returns a list of all unique issue codes registered in the system.
        Used by the ReportController for configuration and filtering.
        """
        return sorted(list(cls._all_codes))