import random
from typing import Callable, TypeVar, Generic, List

T = TypeVar("T")


class ReservoirSampler(Generic[T]):
    """
    An implementation of "Algorithm R" reservoir sampling. This approach
    requires generating a pseudorandom number per item in the input stream, but
    is fast enough for our use cases (we can consider Algorithm L if
    pseudorandom number generation is the bottleneck).
    """

    def __init__(self, sample_size: int, seed: int = 42) -> None:
        self._seed = seed
        self._prng = random.Random(self._seed)
        self._reservoir: List[T] = []
        self._sample_size = sample_size

    def offer(self, item_provider: Callable[[], T]) -> None:
        if len(self._reservoir) < self._sample_size:
            self._reservoir.append(item_provider())
        else:
            j = self._prng.randint(0, len(self._reservoir) - 1)
            if j < self._sample_size:
                self._reservoir[j] = item_provider()

    def get(self) -> List[T]:
        return self._reservoir

    def reset(self) -> None:
        self._prng = random.Random(self._seed)
        self._reservoir.clear()
