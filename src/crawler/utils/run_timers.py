# src/crawler/utils/run_timers.py
import time
from typing import Optional


class RunTimers:
    """
    A simple utility class for measuring elapsed execution time.
    """

    def __init__(self):
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None

    def start(self) -> None:
        """Starts the timer."""
        self._start_time = time.perf_counter()
        self._end_time = None

    def stop(self) -> None:
        """Stops the timer."""
        if self._start_time is not None:
            self._end_time = time.perf_counter()

    @property
    def duration(self) -> float:
        """Returns the elapsed time in seconds."""
        if self._start_time is None:
            return 0.0

        if self._end_time is None:
            # If the timer is still running, return the current duration
            return time.perf_counter() - self._start_time

        # If the timer has stopped, return the final calculated duration
        return self._end_time - self._start_time

    def __repr__(self) -> str:
        """Provides a string representation of the timer's duration."""
        return f"<RunTimers duration={self.duration:.4f}s>"