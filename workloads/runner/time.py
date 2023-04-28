import asyncio
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import final

from attrs import define, field


@final
@define
class TimeMeasurement:
    start_time: datetime
    _elapsed_time: timedelta | None = field(default=None, init=False)

    @property
    def elapsed_time(self) -> timedelta:
        if self._elapsed_time is None:
            raise ValueError("Measurement has not finished yet")
        return self._elapsed_time


@contextmanager
def measure_time() -> Iterator[TimeMeasurement]:
    # Use `datetime.now()` to get actual time
    measurement = TimeMeasurement(start_time=get_current_time())

    # Use `perf_counter()` for more accurate duration
    start_time = time.perf_counter()

    try:
        yield measurement

    finally:
        # Compute elapsed time on exit
        measurement._elapsed_time = timedelta(  # pylint: disable=protected-access
            seconds=time.perf_counter() - start_time
        )


def get_current_time() -> datetime:
    return datetime.now(tz=UTC)


def str_time_to_sec(t) -> int:
    h, m, s = tuple([int(i) for i in t.split(":")])
    return h * 3600 + m * 60 + s


def get_time_delta_in_sec(start_t: str, end_t: str) -> int:
    start_t = str_time_to_sec(start_t)
    end_t = str_time_to_sec(end_t)
    return end_t - start_t


def get_time_delta(start_t: str, end_t: str) -> timedelta:
    delta_t = get_time_delta_in_sec(start_t, end_t)
    return timedelta(seconds=max(0, delta_t))


def get_event_loop_time() -> float:
    return asyncio.get_running_loop().time()
