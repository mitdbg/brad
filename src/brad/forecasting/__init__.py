from typing import List
from datetime import datetime


class Forecaster:
    def at_epochs(
        self, metric_id: str, start_epoch: int, end_epoch: int
    ) -> List[float]:
        raise NotImplementedError

    def at_timestamps(
        self, metric_id: str, start_ts: datetime, end_ts: datetime
    ) -> List[float]:
        raise NotImplementedError
