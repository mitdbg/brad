from typing import Optional
from datetime import datetime
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff


class BackoffHelper:
    def __init__(self) -> None:
        self.backoff: Optional[RandomizedExponentialBackoff] = None
        self.backoff_timestamp: Optional[datetime] = None
