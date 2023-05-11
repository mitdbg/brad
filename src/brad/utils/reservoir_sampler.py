import random
from typing import TypeVar, Generic, List

T = TypeVar("T")


class ReservoirSampler(Generic[T]):
    """
    An implementation of "Algorithm R" reservoir sampling. This approach
    requires generating a pseudorandom number per item in the input stream, but
    is fast enough for our use cases (we can consider Algorithm L if
    pseudorandom number generation is the bottleneck).
    """

    def __init__(self, sample_size: int, seed: int = 42) -> None:
        self._prng = random.Random(seed)
        self._reservoir = []
        self._sample_size = sample_size

    def offer(self, item: T) -> None:
        if len(self._reservoir) < self._sample_size:
            self._reservoir.append(item)
        else:
            j = self._prng.randint(0, len(self._reservoir) - 1)
            if j < self._sample_size:
                self._reservoir[j] = item

    def get(self) -> List[T]:
        return self._reservoir
