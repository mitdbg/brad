import asyncio
import logging
import select
import sys
import threading
import traceback
from datetime import timedelta
from typing import Optional

from brad.utils.sentinel import Sentinel
from brad.utils.time_periods import universal_now

logger = logging.getLogger(__name__)


class Watchdog:
    def __init__(
        self,
        check_period: timedelta,
        take_action_after: timedelta,
    ) -> None:
        self._check_period_s = check_period.total_seconds()
        self._take_action_after = take_action_after
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._sentinel = Sentinel()
        self._thread = threading.Thread(name="Watchdog", target=self._thread_main)
        self._lock = threading.Lock()
        self._last_ping = universal_now()

    def start(self, event_loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._event_loop = event_loop
        self._sentinel.start()
        self.ping()
        self._thread.start()

    def stop(self) -> None:
        self._sentinel.signal_exit()
        self._thread.join()
        self._sentinel.stop()

    def ping(self) -> None:
        with self._lock:
            self._last_ping = universal_now()

    def _should_take_action(self) -> bool:
        now = universal_now()
        with self._lock:
            last_ping = self._last_ping
        diff = now - last_ping
        return diff > self._take_action_after

    def _log_thread_state(self) -> None:
        logger.info("Watchdog thread stack dump")
        # pylint: disable-next=protected-access
        for thread_id, _ in threading._active.items():  # type: ignore
            logger.info("Thread ID: %d", thread_id)
            # pylint: disable-next=protected-access
            frames = sys._current_frames()[thread_id]
            for f in frames:
                logger.info("\n".join(traceback.format_stack(f)))
            logger.info("")
        logger.info("")

    def _log_async_tasks(self) -> None:
        if self._event_loop is None:
            logger.info("Watchdog: No asyncio event loop.")
            return

        logger.info("Watchdog asyncio task dump")
        for task in asyncio.all_tasks(self._event_loop):
            logger.info("Task ID: %d", id(task))
            frames = task.get_stack()
            for f in frames:
                logger.info("\n".join(traceback.format_stack(f)))
            logger.info("")
        logger.info("")

    def _thread_main(self) -> None:
        while True:
            read_ready, _, _ = select.select(
                [self._sentinel.read_pipe], [], [], self._check_period_s
            )
            if self._sentinel.should_exit(read_ready):
                self._sentinel.consume_exit_signal()
                break

            if self._should_take_action():
                logger.info("Watchdog triggering.")
                self._log_thread_state()
                self._log_async_tasks()
