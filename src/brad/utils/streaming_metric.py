from datetime import datetime
from collections import deque
from typing import Deque, Tuple, TypeVar, Generic, Optional, Iterator

T = TypeVar("T")
R = TypeVar("R", int, float)  # Numeric


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
        super().__init__()
        self._metric_data: Deque[Tuple[T, datetime]] = deque()
        self._window_size = window_size

    def add_sample(self, value: T, timestamp: datetime) -> None:
        self._metric_data.append((value, timestamp))
        self._trim_metric_data()

    def window_iterator(
        self, start: datetime, end: datetime
    ) -> Iterator[Tuple[T, datetime]]:
        """
        Emits all metric values that intersect with the provided interval.
        """
        collecting = False

        # Assumption is that `metric_data` is sorted in ascending timestamp order.
        for value, val_timestamp in self._metric_data:
            if collecting:
                yield value, val_timestamp
                if val_timestamp >= end:
                    break

            else:
                if val_timestamp >= start:
                    collecting = True
                    yield value, val_timestamp

                    if val_timestamp >= end:
                        # Edge case, when a window falls inside a single
                        # sample's range.
                        break

    def average_since(self, timestamp: datetime) -> T:
        raise NotImplementedError

    def most_recent_in_window(self, start: datetime, end: datetime) -> Optional[T]:
        raise NotImplementedError

    def average_in_window(self, start: datetime, end: datetime) -> T:
        raise NotImplementedError

    def sum_in_window(self, start: datetime, end: datetime) -> T:
        raise NotImplementedError

    def max_in_window(self, start: datetime, end: datetime) -> T:
        raise NotImplementedError

    def _trim_metric_data(self) -> None:
        while len(self._metric_data) > self._window_size:
            self._metric_data.popleft()

    def _reverse_interval_iterator(self) -> Iterator[Tuple[T, datetime, datetime]]:
        # Assumption is that `metric_data` is sorted in ascending timestamp order.
        rit = reversed(self._metric_data)
        prev = None

        try:
            curr = next(rit)
            while True:
                if prev is not None:
                    yield prev[0], curr[1], prev[1]
                prev = curr
                curr = next(rit)
        except StopIteration:
            pass

        if prev is not None:
            yield (prev[0], datetime.min.replace(tzinfo=prev[1].tzinfo), prev[1])


class StreamingNumericMetric(StreamingMetric[float]):
    def __init__(self, empty_value: float = 0.0, window_size: int = 10) -> None:
        super().__init__(window_size)
        self._empty_value = empty_value

    def average_since(self, timestamp: datetime) -> float:
        total = None
        num_samples = 0
        for value, _ in self.window_iterator(timestamp, datetime.max):
            if total is None:
                total = value
            else:
                total += value
            num_samples += 1

        if total is None:
            return self._empty_value

        return total / num_samples

    def most_recent_in_window(self, start: datetime, end: datetime) -> Optional[float]:
        if len(self._metric_data) == 0:
            return self._empty_value

        for value, left, right in self._reverse_interval_iterator():
            # Check for intersection. We negate the cases where there is no intersection.
            if not ((right < start) or (end <= left)):
                return value

        # Reaching here means that all of our samples are older than the
        # provided interval.
        return None

    def average_in_window(self, start: datetime, end: datetime) -> float:
        total = None
        num_samples = 0

        for value, _ in self.window_iterator(start, end):
            if total is None:
                total = value
            else:
                total += value
            num_samples += 1

        if total is None:
            return self._empty_value
        else:
            return total / num_samples

    def sum_in_window(self, start: datetime, end: datetime) -> float:
        total = None

        for value, _ in self.window_iterator(start, end):
            if total is None:
                total = value
            else:
                total += value

        return total if total is not None else self._empty_value

    def max_in_window(self, start: datetime, end: datetime) -> float:
        largest = None
        for val, _ in self.window_iterator(start, end):
            if largest is None:
                largest = val
            else:
                largest = max(largest, val)

        if largest is None:
            return self._empty_value
        else:
            return largest
