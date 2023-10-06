import heapq
from typing import Generic, TypeVar, Deque, List
from collections import deque, namedtuple

T = TypeVar("T", int, float)

RunTimeSummary = namedtuple("RunTimeSummary", ["sum", "num_values", "top_k"])


class RunTimeReservoir(Generic[T]):
    """
    Keeps a fixed sized set of values in memory, given by a stream. This class
    does not use any fancy data structures; it is meant to be used for small
    reservoirs.
    """

    def __init__(self, reservoir_size: int) -> None:
        self._values: Deque[T] = deque()
        self._reservoir_size = reservoir_size

    def add_value(self, value: T) -> None:
        if len(self._values) >= self._reservoir_size:
            self._values.popleft()
        self._values.append(value)

    def clear(self) -> None:
        self._values.clear()

    def get_summary(self, k: int) -> RunTimeSummary:
        total: T = 0
        max_heap: List[T] = []
        for val in self._values:
            total += val
            if len(max_heap) < k:
                max_heap.append(-val)
                if len(max_heap) == k:
                    heapq.heapify(max_heap)
            elif val > -max_heap[0]:
                heapq.heappushpop(max_heap, -val)

        for idx in range(len(max_heap)):
            max_heap[idx] = -max_heap[idx]

        return RunTimeSummary(total, len(self._values), max_heap)
