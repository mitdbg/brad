from typing import List
from datetime import datetime


class Forecaster:
    def num_points(self, metric_id: str, num_points: int) -> List[float]:
        raise NotImplementedError

    def until(self, metric_id: str, end_ts: datetime) -> List[float]:
        raise NotImplementedError
