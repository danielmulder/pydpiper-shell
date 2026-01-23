# src/pydpiper_shell/core/loop_runner.py
from __future__ import annotations

import asyncio
import threading
from typing import Optional, Any

_MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None
_THREAD: Optional[threading.Thread] = None


def ensure_background_loop() -> None:
    """
    Ensures a persistent asyncio event loop is running on a background thread.
    If the loop is already running, this function does nothing.
    """
    global _MAIN_LOOP, _THREAD
    if _MAIN_LOOP is not None:
        return

    # Create a new event loop
    loop = asyncio.new_event_loop()

    def _run_loop(loop_: asyncio.AbstractEventLoop) -> None:
        """Sets the loop and runs it until stop() is called."""
        asyncio.set_event_loop(loop_)
        loop_.run_forever()

    # Start the loop in a dedicated, daemonized thread
    t = threading.Thread(target=_run_loop, args=(loop,), daemon=True)
    t.start()

    _MAIN_LOOP = loop
    _THREAD = t


def run_on_main_loop(coro: "asyncio.coroutines.coroutine[Any, Any, Any]", timeout: float | None = None) -> Any:
    """
    Executes a coroutine on the persistent background loop and waits for the result.
    Falls back to asyncio.run() if no background loop exists (for compatibility).

    Args:
        coro: The coroutine to execute.
        timeout (float | None): Optional timeout in seconds to wait for the result.

    Returns:
        Any: The result of the coroutine.
    """
    global _MAIN_LOOP
    if _MAIN_LOOP is not None:
        # Submit coroutine to the background loop and block until result is available
        fut = asyncio.run_coroutine_threadsafe(coro, _MAIN_LOOP)
        return fut.result(timeout)

    # Fallback in case the loop was not started (e.g., in a simple test context)
    return asyncio.run(coro)