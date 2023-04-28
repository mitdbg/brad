from brad.forecasting import Forecaster
import pandas as pd
from typing import List
from datetime import datetime, timedelta


class MovingAverageForecaster(Forecaster):
    def __init__(
        self, df: pd.DataFrame, epoch_length: timedelta, window_size: int = 5
    ) -> None:
        self._df = df
        self._epoch_length = epoch_length
        self._window_size = window_size

    def update_df_pointer(self, df: pd.DataFrame) -> None:
        self._df = df

    # Returns empty list if `num_points` is <= 0
    def num_points(self, metric_id: str, num_points: int) -> List[float]:
        return max(num_points, 0) * [self._df.tail(self._window_size).mean()[metric_id]]

    # `end_ts` is inclusive
    # Returns empty list if `end_ts` is sooner than one `_epoch_length` after the last entry in `_df`.
    def until(self, metric_id: str, end_ts: datetime) -> List[float]:
        num_points = (end_ts - self._df.index[-1]) // self._epoch_length
        return self.num_points(metric_id, num_points)
