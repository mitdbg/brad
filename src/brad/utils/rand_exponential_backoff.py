import random

from typing import Optional


class RandomizedExponentialBackoff:
    def __init__(
        self,
        max_retries: int,
        base_delay_s: float,
        max_delay_s: float,
        exp_base: float = 2.0,
        jitter: bool = True,
    ) -> None:
        self._max_retries = max_retries
        self._base_delay_s = base_delay_s
        self._max_delay_s = max_delay_s
        self._exp_base = exp_base
        self._jitter = jitter
        self._retries = 0
        self._prng = random.Random()

    def wait_time_s(self) -> Optional[float]:
        """
        Returns the number of seconds to wait. If this method returns `None`, it
        indicates exceeding the number of retries.
        """
        if self._retries < self._max_retries:
            delay = min(
                self._base_delay_s * (self._exp_base**self._retries),
                self._max_delay_s,
            )

            if self._jitter:
                jitter_range = delay * 0.2
                delay += self._prng.uniform(-jitter_range, jitter_range)

            self._retries += 1
            return delay
        else:
            return None
