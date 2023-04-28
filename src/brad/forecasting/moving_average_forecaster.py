from brad.forecasting import Forecaster
import pandas as pd
from typing import List
from datetime import datetime, timedelta


class MovingAverageForecaster(Forecaster):
    def __init__(self, df: pd.DataFrame, epoch_length: timedelta) -> None:
        raise NotImplementedError

    def update_df_pointer(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    # Returns empty list if `num_points` is <= 0
    def num_points(self, metric_id: str, num_points: int) -> List[float]:
        raise NotImplementedError

    # `end_ts` is inclusive
    # Returns empty list if `end_ts` is sooner than one `_epoch_length` after the last entry in `_df`.
    def until(self, metric_id: str, end_ts: datetime) -> List[float]:
        raise NotImplementedError
