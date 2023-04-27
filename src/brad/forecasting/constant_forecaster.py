from brad.forecasting import Forecaster
import pandas as pd
from typing import List
from datetime import datetime, timedelta
import numpy as np
import math


class ConstantForecaster(Forecaster):
    def __init__(self, df: pd.DataFrame, epoch_length: timedelta) -> None:
        self._df = df
        self._epoch_length = epoch_length

    # Returns empty list if `num_points` is <= 0
    def num_points(self, metric_id: str, num_points: int) -> List[float]:
        return max(num_points, 0) * list(self._df.tail(1)[metric_id])

    # `end_ts` is inclusive
    # Returns empty list if `end_ts` is sooner than one `_epoch_length` after the last entry in `_df`.
    def until(self, metric_id: str, end_ts: datetime) -> List[float]:
        num_points = (end_ts - self._df.index[-1]) // self._epoch_length
        return self.num_points(metric_id, num_points)


if __name__ == "__main__":
    dataframe = pd.DataFrame(
        {"a": np.random.randint(1, 10, 10), "b": np.random.randint(1, 10, 10)},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = ConstantForecaster(dataframe, timedelta(days=1))
    print(dataframe)

    print("----\nTest num_points()\n----")

    print(f.num_points("a", -3))
    print(f.num_points("a", 0))
    print(f.num_points("a", 3))
    print(f.num_points("b", 4))

    print("----\nTest until()\n----")

    print(
        f.until(
            "a",
            datetime(year=2022, month=1, day=8),
        )
    )
    print(
        f.until(
            "a",
            datetime(year=2022, month=1, day=10),
        )
    )
    print(
        f.until(
            "a",
            datetime(year=2022, month=1, day=14),
        )
    )

    print(
        f.until(
            "a",
            datetime(year=2022, month=1, day=16, hour=5),
        )
    )
    print(
        f.until(
            "b",
            datetime(year=2022, month=1, day=17, hour=4),
        )
    )
