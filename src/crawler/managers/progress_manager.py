# src/crawler/managers/progress_manager.py
import sys
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)


class ProgressManager:
    """
    Manages the complete lifecycle of a tqdm progress bar.
    """

    def __init__(self, total: int, desc: str, unit: str = "it", max_pages: int = None):
        """
        Initializes and displays the progress bar.

        Args:
            total: Initial total of items (URLs) to process.
            max_pages: Optional limit on successful pages to crawl.
        """
        if total <= 0:
            total = 1

        self.max_pages = max_pages

        # Bepaal initiële postfix string
        pages_str = "0"
        #if self.max_pages is not None:
        #    pages_str = f"0/{self.max_pages}"

        self.pbar = tqdm(
            total=total,
            desc=desc,
            unit=f" {unit}",
            dynamic_ncols=True,
            smoothing=0.1,
            mininterval=0.5,
            postfix={"pages": pages_str, "failures": 0},
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
            file=sys.stdout
        )

    def advance(self, steps: int = 1, pages_count: int = None, failures_count: int = None):
        """
        Increments the progress bar (steps = processed URLs) and updates status counters.
        """
        if self.pbar:
            self.pbar.update(steps)

            current_postfix = self.pbar.postfix or {}
            if not isinstance(current_postfix, dict):
                current_postfix = {}

            updated = False

            # Update Pages met Max Pages logica
            if pages_count is not None:
                if self.max_pages:
                    # Toon: "150/500"
                    current_postfix["pages"] = f"{pages_count}/{self.max_pages}"
                else:
                    # Toon: "150"
                    current_postfix["pages"] = str(pages_count)
                updated = True

            if failures_count is not None:
                current_postfix["failures"] = failures_count
                updated = True

            if updated:
                self.pbar.set_postfix(current_postfix, refresh=False)

    def update_total(self, increment: int = 1):
        """Increments the total count of URLs in the progress bar."""
        if self.pbar and self.pbar.total is not None:
            self.pbar.total += increment
            self.pbar.refresh()

    def set_total(self, new_total: int):
        """Sets the total of the progress bar (URLs found)."""
        if self.pbar:
            if new_total < self.pbar.n:
                self.pbar.total = self.pbar.n
            else:
                self.pbar.total = new_total
            self.pbar.refresh()

    def close(self, final_pages: int, final_failures: int = 0, capped: bool = False):
        """
        Closes the progress bar with correct final status.

        Args:
            capped: True if the crawl stopped because max_pages was reached.
        """
        if not self.pbar:
            return

        try:
            # Final format logic
            if capped and self.max_pages:
                final_pages_str = f"{final_pages}/capped"
            else:
                # Als niet gecapped (dus queue leeg), laten we de "/500" wegvallen
                final_pages_str = str(final_pages)

            self.pbar.set_postfix({
                "pages": final_pages_str,
                "failures": final_failures
            }, refresh=True)

            self.pbar.close()
            logger.debug("ProgressManager: Progress bar closed.")
        except Exception as e:
            logger.error(f"Error encountered while closing progress bar: {e}")