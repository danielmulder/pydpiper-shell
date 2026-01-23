# src/crawler/managers/adaptive_worker_manager.py
import asyncio
import logging
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)


class AdaptiveWorkerManager:
    """
    Manages a pool of asynchronous workers to process items from a queue.
    Supports stopping (shutdown) and pausing (temporary halt e.g. for 429s).
    """

    def __init__(self,
                 work_coro: Callable[[any], Awaitable[None]],
                 queue: asyncio.Queue,
                 concurrency: int,
                 stop_event: asyncio.Event,
                 pause_event: asyncio.Event = None):
        """
        Initializes the WorkerManager.

        Args:
            work_coro: The async function to execute for each item.
            queue: The asyncio.Queue to pull items from.
            concurrency: The number of parallel workers.
            stop_event: An event to signal workers to shut down.
            pause_event: An event to signal workers to pause temporarily (e.g. 429 backoff).
                         If not provided, creates an internal event set to True (always run).
        """
        self.work_coro = work_coro
        self.queue = queue
        self.stop_event = stop_event
        self.concurrency = concurrency

        # Als er geen pause_event wordt meegegeven, maken we er een die standaard op 'Groen' staat.
        if pause_event is None:
            self.pause_event = asyncio.Event()
            self.pause_event.set()
        else:
            self.pause_event = pause_event

        self._tasks: list[asyncio.Task] = []
        self._has_started = False

    def is_idle(self) -> bool:
        """Checks if all workers have finished their tasks."""
        return all(task.done() for task in self._tasks)

    async def run(self):
        """Starts the worker pool and waits for them to complete."""
        if self._has_started:
            logger.warning("WorkerManager already running.")
            return

        self._has_started = True
        logger.debug(f"Starting {self.concurrency} workers...")

        self._tasks = [
            asyncio.create_task(self._worker_loop(f"Worker-{i + 1}"))
            for i in range(self.concurrency)
        ]

        # Wacht tot alle worker-taken daadwerkelijk zijn voltooid.
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.debug("All workers have stopped and gathered.")

    async def _worker_loop(self, name: str):
        """
        The main loop for an individual worker task.
        Respects stop_event for shutdown and pause_event for temporary halts.
        """
        logger.debug(f"[{name}] Started.")

        while True:
            # 1. CHECK STOP EVENT (Prioriteit 1)
            if self.stop_event.is_set():
                break

            # 2. CHECK PAUSE EVENT (Prioriteit 2)
            # Als de slagboom dicht is (pause_event.clear()), wachten we hier.
            if not self.pause_event.is_set():
                # logger.debug(f"[{name}] Paused by pause_event...") # Optioneel, kan veel spam geven
                try:
                    await self.pause_event.wait()
                    # Als we wakker worden, checken we meteen of we niet alsnog moeten stoppen
                    if self.stop_event.is_set():
                        break
                    # logger.debug(f"[{name}] Resumed.")
                except asyncio.CancelledError:
                    break

            # 3. VERWERK QUEUE
            item = None
            try:
                # Wacht maximaal 0.5s op een item. Dit zorgt dat we regelmatig
                # de stop_event en pause_event opnieuw controleren als de queue leeg is.
                item = await asyncio.wait_for(self.queue.get(), timeout=0.5)

            except asyncio.TimeoutError:
                # Geen item, check condities opnieuw in volgende iteratie
                continue

            except asyncio.CancelledError:
                logger.debug(f"[{name}] Task cancelled.")
                break

            except Exception as e:
                logger.error(f"[{name}] Unexpected error during queue.get(): {e}", exc_info=True)
                await asyncio.sleep(0.5)
                continue

            # Als we hier zijn, hebben we een item en moeten we aan het werk
            try:
                await self.work_coro(item)

            except asyncio.CancelledError:
                logger.debug(f"[{name}] Cancelled during work_coro.")
                if item is not None:
                    self.queue.task_done()
                break

            except Exception:
                logger.exception(f"[{name}] Unhandled exception processing item {str(item)}.")
                # Zelfs bij een crash moeten we task_done aanroepen
                if item is not None:
                    self.queue.task_done()

            finally:
                # Markeer taak als gedaan, zodat queue.join() werkt
                if item is not None:
                    self.queue.task_done()

        logger.debug(f"[{name}] Stopped.")