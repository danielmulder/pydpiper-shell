import asyncio
import logging

logger = logging.getLogger(__name__)


class AsyncController:
    """
    Base class for asynchronous controllers.

    Provides lifecycle hooks (setup, shutdown) and resource cleanup
    for managing long-running asynchronous tasks.
    """

    def __init__(self):
        """Initializes the controller's state."""
        self._setup_done = False
        self._worker_task: asyncio.Task | None = None
        self.stop_crawl_event = asyncio.Event()

    async def setup(self):
        """
        Prepares resources for the controller.

        This method can be overridden in a subclass to perform specific
        initialization tasks. It ensures that setup is only run once.
        """
        if self._setup_done:
            return
        self._setup_done = True
        logger.debug("AsyncController setup done.")

    async def shutdown(self):
        """Gracefully shuts down resources and cancels running tasks."""
        try:
            if self._worker_task and not self._worker_task.done():
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    logger.debug("Worker task cancelled cleanly.")
        except Exception as e:
            logger.error("Error during controller shutdown: %s", e, exc_info=True)