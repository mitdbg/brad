from datetime import datetime
from collections import deque
from typing import Deque, Tuple, TypeVar, Generic

T = TypeVar("T", int, float)


class StreamingMetric(Generic[T]):
    """
    A simple wrapper around a time-series metric. The assumption is that this
    metric is updated at some regular frequency.

    The values in the metric represent the sampled metric at the given timestamp
    to the previous timestamp, exclusive. Pictorially:

      ---]---]---]

    Where `]` represents the sample.
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
            if val_timestamp < timestamp:
                break

            if total is None:
                total = value
            else:
                total += value
            num_samples += 1

        if total is None:
            return 0.0

        return total / num_samples

    def average_in_window(self, start: datetime, end: datetime) -> float:
        if len(self._metric_data) == 0:
            return 0.0

        total = None
        num_samples = 0
        collecting = False

        # Assumption is that `metric_data` is sorted in ascending timestamp order.
        for value, val_timestamp in self._metric_data:
            if collecting:
                assert total is not None
                total += value
                num_samples += 1

                if val_timestamp >= end:
                    break

            else:
                if val_timestamp >= start:
                    collecting = True
                    total = value
                    num_samples += 1

                    if val_timestamp >= end:
                        # Edge case, when a window falls inside a single
                        # sample's range.
                        break

        if total is None:
            return 0.0
        else:
            return total / num_samples

    def _trim_metric_data(self) -> None:
        while len(self._metric_data) > self._window_size:
            self._metric_data.popleft()
