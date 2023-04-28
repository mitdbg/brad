from typing import List
from datetime import datetime
import pandas as pd


class Forecaster:
    def update_df_pointer(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def num_points(self, metric_id: str, num_points: int) -> List[float]:
        raise NotImplementedError

    def until(self, metric_id: str, end_ts: datetime) -> List[float]:
        raise NotImplementedError
