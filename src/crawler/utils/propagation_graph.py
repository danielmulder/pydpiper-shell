"""
PropagationGraph module.

Provides a small directed-graph utility to store internal links and propagate
HTTP-like status codes through that graph. All comments and docstrings are
in English and follow PEP conventions.
"""
from collections import defaultdict, deque
import logging
from typing import Dict, Set, Optional, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


class PropagationGraph:
    """Directed adjacency-list graph that propagates numeric status codes.

    The graph stores edges as source_url -> set(target_url) and keeps a mapping
    of url -> status_code. The propagation rule used here is:
      * When propagating, the higher numeric status value "wins" (i.e. is
        considered worse). For example, 500 > 404 > 200.
    """

    def __init__(self) -> None:
        """Initialize adjacency list and status mapping."""
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.statuses: Dict[str, int] = {}

    def add_link(self, source_url: str, target_url: str) -> None:
        """Add a directed edge from source_url to target_url."""
        self.graph[source_url].add(target_url)

    def set_status(self, url: str, status: Any) -> None:
        """Set the numeric status for a URL.

        The function attempts to coerce the provided status to int. If the
        coercion fails, the call will be ignored and a warning will be logged.
        Only non-negative integers are stored.
        """
        try:
            status_int = int(status)
        except (TypeError, ValueError):
            logger.warning("Could not convert status '%s' for URL '%s' to int.", status, url)
            return

        if status_int < 0:
            logger.warning("Ignoring negative status %s for URL '%s'.", status_int, url)
            return

        self.statuses[url] = status_int

    def propagate_statuses(self) -> None:
        """Propagate statuses through the graph.

        Uses a queue (deque) to perform a BFS-like propagation. When a source
        URL has a higher numeric status than a target URL, the target is
        overwritten and scheduled for further propagation.
        """
        queue = deque(self.statuses.keys())
        visited_count = 0

        while queue:
            current_url = queue.popleft()
            current_status = self.statuses.get(current_url)

            # Skip URLs without a defined status.
            if current_status is None:
                continue

            for linked_url in self.graph.get(current_url, ()):
                existing = self.statuses.get(linked_url)
                # Overwrite target status if current_status is higher (worse)
                if existing is None or existing < current_status:
                    self.statuses[linked_url] = current_status
                    queue.append(linked_url)
                    visited_count += 1

        logger.debug("Propagation completed; %d updates applied.", visited_count)

    def get_status(self, url: str) -> Optional[int]:
        """Return the stored status for a URL, or None if unknown."""
        return self.statuses.get(url)

    def get_all_statuses(self) -> Dict[str, int]:
        """Return a shallow copy of all URL -> status mappings."""
        return dict(self.statuses)
