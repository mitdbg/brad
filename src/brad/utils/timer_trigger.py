import select

from typing import Callable
from threading import Thread
from brad.utils.sentinel import Sentinel


class TimerTrigger:
    def __init__(self, period_s: float, to_run: Callable):
        """
        This class calls `to_run` periodically. The time between successive
        calls to `to_run` will be at least `period_s` (can be longer).
        """
        self._period_s = period_s
        self._to_run = to_run
        self._timer = Thread(target=self._timer_run)
        self._sentinel = Sentinel()

    def start(self):
        self._sentinel.start()
        self._timer.start()

    def stop(self):
        self._sentinel.signal_exit()
        self._timer.join()
        self._sentinel.stop()

    def _timer_run(self):
        while True:
            read_ready, _, _ = select.select(
                [self._sentinel.read_pipe], [], [], self._period_s
            )
            if self._sentinel.should_exit(read_ready):
                self._sentinel.consume_exit_signal()
                break
            self._to_run()
