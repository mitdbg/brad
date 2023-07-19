from datetime import datetime
from collections import deque
from typing import Deque, Tuple, TypeVar, Generic

T = TypeVar("T", int, float)


class StreamingMetric(Generic[T]):
    """
    A simple wrapper around a time-series metric. The assumption is that this
    metric is updated at some regular frequency.
    """

    def __init__(self, window_size: int = 10) -> None:
        self._metric_data: Deque[Tuple[T, datetime]] = deque()
        self._window_size = window_size

    def add_sample(self, value: T, timestamp: datetime) -> None:
        self._metric_data.append((value, timestamp))
        self._trim_metric_data()

    def average_since(self, timestamp: datetime) -> float:
        if len(self._metric_data) == 0:
            return 0.0

        # Assumption is that `metric_data` is sorted in ascending timestamp order.
        total = None
        num_samples = 0
        for value, val_timestamp in reversed(self._metric_data):
            if total is None:
                total = value
            else:
                total += value
            num_samples += 1

            if val_timestamp <= timestamp:
                # We want to add the first value with a timestamp less than or
                # equal to the given timestamp.
                break

        assert total is not None
        return total / num_samples

    def _trim_metric_data(self) -> None:
        while len(self._metric_data) > self._window_size:
            self._metric_data.popleft()
