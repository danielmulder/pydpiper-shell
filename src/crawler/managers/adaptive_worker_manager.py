"""
Adaptive Worker Manager
Handles a pool of asynchronous workers with support for pausing and stopping.
"""

import asyncio
import logging
from typing import Callable, Awaitable, Any, List, Optional

logger = logging.getLogger(__name__)


class AdaptiveWorkerManager:
    """
    Manages a pool of asynchronous workers to process items from an asyncio.Queue.

    Supports graceful shutdown via stop_event and temporary execution halts
    (e.g., for 429 rate-limiting backoff) via pause_event.
    """

    def __init__(
            self,
            work_coro: Callable[[Any], Awaitable[None]],
            queue: asyncio.Queue,
            concurrency: int,
            stop_event: asyncio.Event,
            pause_event: Optional[asyncio.Event] = None
    ):
        """
        Initialize the AdaptiveWorkerManager.

        Args:
            work_coro: Asynchronous function to execute for each queue item.
            queue: The queue to retrieve items from.
            concurrency: Number of parallel worker tasks to spawn.
            stop_event: Signal to shut down all workers.
            pause_event: Signal to temporarily halt processing.
                         If None, an internal event is created (defaults to running).
        """
        self.work_coro = work_coro
        self.queue = queue
        self.concurrency = concurrency
        self.stop_event = stop_event

        # Initialize pause_event. If not provided, default to 'True' (Always Running).
        if pause_event is None:
            self.pause_event = asyncio.Event()
            self.pause_event.set()
        else:
            self.pause_event = pause_event

        self._tasks: List[asyncio.Task] = []
        self._has_started: bool = False

    def is_idle(self) -> bool:
        """Check if all spawned worker tasks have finished execution."""
        return all(task.done() for task in self._tasks)

    async def run(self) -> None:
        """
        Start the worker pool and wait for all tasks to complete.
        """
        if self._has_started:
            logger.warning("WorkerManager already running.")
            return

        self._has_started = True
        logger.debug("Starting %d workers...", self.concurrency)

        self._tasks = [
            asyncio.create_task(self._worker_loop(f"Worker-{i + 1}"))
            for i in range(self.concurrency)
        ]

        # Wait for all worker tasks to finish or raise exceptions
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.debug("All workers have been shut down and gathered.")

    async def _worker_loop(self, name: str) -> None:
        """
        Internal worker loop for individual task execution.

        Continuously checks the stop_event for shutdown and pause_event for
        throttling before attempting to pull from the queue.
        """
        logger.debug("[%s] Started.", name)

        while True:
            # 1. PRIORITY CHECK: Shutdown
            if self.stop_event.is_set():
                break

            # 2. SECONDARY CHECK: Throttling / Pause
            # If pause_event is cleared (e.g., by a 429 handler), workers wait here.
            if not self.pause_event.is_set():
                try:
                    await self.pause_event.wait()
                    # Re-check shutdown immediately after waking up
                    if self.stop_event.is_set():
                        break
                except asyncio.CancelledError:
                    break

            # 3. QUEUE PROCESSING
            item = None
            try:
                # Use a short timeout to allow frequent re-checks of stop/pause events
                item = await asyncio.wait_for(self.queue.get(), timeout=0.5)

            except asyncio.TimeoutError:
                # No item in queue, restart loop to check conditions
                continue

            except asyncio.CancelledError:
                logger.debug("[%s] Task cancelled during queue retrieval.", name)
                break

            except Exception as e:
                logger.error("[%s] Unexpected queue error: %s", name, e, exc_info=True)
                await asyncio.sleep(0.5)
                continue

            # 4. EXECUTE COROUTINE
            try:
                await self.work_coro(item)

            except asyncio.CancelledError:
                logger.debug("[%s] Task cancelled during execution of work_coro.", name)
                # Cleanup: ensure task_done is called if the item was retrieved
                if item is not None:
                    self.queue.task_done()
                break

            except Exception:
                logger.exception("[%s] Unhandled exception processing item: %s", name, item)

            finally:
                # Always mark task as done to avoid blocking queue.join()
                if item is not None:
                    self.queue.task_done()

        logger.debug("[%s] Stopped.", name)