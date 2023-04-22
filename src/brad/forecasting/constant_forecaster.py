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

    # Start inclusive, end exclusive
    def at_epochs(
        self, metric_id: str, start_epoch: int, end_epoch: int
    ) -> List[float]:
        if start_epoch >= end_epoch:
            raise ValueError("The end epoch must be greater than the start epoch.")

        # Retrieve values for observed epochs
        existing = []
        if start_epoch < 0:
            existing = list(self._df.tail(-start_epoch)[metric_id])
            if end_epoch < 0:
                existing = existing[: (end_epoch - start_epoch)]

        # Append constant values for future epochs
        no_vals_required = max(0, end_epoch) - max(0, start_epoch)
        forecasted = no_vals_required * list(self._df.tail(1)[metric_id])

        return existing + forecasted

    # Start inclusive, end exclusive
    def at_timestamps(
        self, metric_id: str, start_ts: datetime, end_ts: datetime
    ) -> List[float]:
        if start_ts >= end_ts:
            raise ValueError(
                "The end timestamp must be greater than the start timestamp."
            )

        # Retrieve values for observed timestamps
        existing = list(
            self._df.loc[(self._df.index >= start_ts) & (self._df.index < end_ts)][
                metric_id
            ]
        )

        # Append constant values for future epochs
        base = self._df.index[-1]
        aligned_start_ts = base + self._epoch_length * math.ceil(
            (start_ts - base) / self._epoch_length
        )
        aligned_end_ts = base + self._epoch_length * math.ceil(
            (end_ts - base) / self._epoch_length
        )
        no_vals_required = (
            max(base + self._epoch_length, aligned_end_ts)
            - max(base + self._epoch_length, aligned_start_ts)
        ) // self._epoch_length
        forecasted = no_vals_required * list(self._df.tail(1)[metric_id])

        return existing + forecasted


if __name__ == "__main__":
    df = pd.DataFrame(
        {"a": np.random.randint(1, 10, 10), "b": np.random.randint(1, 10, 10)},
        index=pd.date_range("2022-01-01", "2022-01-10", freq="D", normalize=True),
    )

    f = ConstantForecaster(df, timedelta(days=1))
    print(df)

    print("----\nTest at_epochs()\n----")

    print(f.at_epochs("a", -5, -3))
    print(f.at_epochs("a", -5, 0))
    print(f.at_epochs("a", -5, 3))
    print(f.at_epochs("a", 0, 4))
    print(f.at_epochs("a", 2, 5))

    print("----\nTest at_timestamps()\n----")

    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=6),
            datetime(year=2022, month=1, day=8),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=6),
            datetime(year=2022, month=1, day=11),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=6),
            datetime(year=2022, month=1, day=14),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=11),
            datetime(year=2022, month=1, day=15),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=13),
            datetime(year=2022, month=1, day=16),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=13),
            datetime(year=2022, month=1, day=16, hour=5),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=13, hour=5),
            datetime(year=2022, month=1, day=16, hour=5),
        )
    )
    print(
        f.at_timestamps(
            "a",
            datetime(year=2022, month=1, day=13, hour=0),
            datetime(year=2022, month=1, day=17, hour=4),
        )
    )
