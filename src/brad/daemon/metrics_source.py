import pandas as pd
from typing import List
from datetime import datetime, timedelta

from brad.forecasting import Forecaster
from brad.forecasting.constant_forecaster import ConstantForecaster
from brad.forecasting.moving_average_forecaster import MovingAverageForecaster
from brad.forecasting.linear_forecaster import LinearForecaster


class MetricsSourceWithForecasting:
    """
    Represents a source of metrics that can be periodically fetched and also
    forecasted.
    """

    def __init__(
        self,
        epoch_length: timedelta,
        forecasting_method: str,
        forecasting_window_size: int,
    ) -> None:
        self._epoch_length = epoch_length
        values = self._metrics_values()
        self._forecaster: Forecaster
        if forecasting_method == "constant":
            self._forecaster = ConstantForecaster(values, self._epoch_length)
        elif forecasting_method == "moving_average":
            self._forecaster = MovingAverageForecaster(
                values, self._epoch_length, forecasting_window_size
            )
        elif forecasting_method == "linear":
            self._forecaster = LinearForecaster(
                values, self._epoch_length, forecasting_window_size
            )

    async def fetch_latest(self) -> None:
        """
        Retrieves the latest metric values from the underlying source (e.g.,
        CloudWatch). This should be called at least once every `epoch_length`.
        """
        self._forecaster.update_df_pointer(self._metrics_values())

    def _metrics_values(self) -> pd.DataFrame:
        raise NotImplementedError

    # Metrics retrieval methods.

    def read_k_most_recent(
        self, k: int = 1, metric_ids: List[str] | None = None
    ) -> pd.DataFrame:
        values = self._metrics_values()
        if values.empty:
            return values

        columns = metric_ids if metric_ids else list(values.columns)

        return values.tail(k)[columns]

    def read_k_upcoming(
        self, k: int = 1, metric_ids: List[str] | None = None
    ) -> pd.DataFrame:
        values = self._metrics_values()
        if values.empty:
            return values

        # Create empty dataframe with desired index and columns
        timestamps = [
            values.index[-1] + i * self._epoch_length for i in range(1, k + 1)
        ]
        columns = metric_ids if metric_ids else values.columns
        df = pd.DataFrame(index=timestamps, columns=columns)

        # Fill in the values
        for col in columns:
            vals = self._forecaster.num_points(col, k)
            df[col] = vals

        return df

    # `end_ts` is inclusive
    def read_upcoming_until(
        self, end_ts: datetime, metric_ids: List[str] | None = None
    ) -> pd.DataFrame:
        values = self._metrics_values()
        if values.empty:
            return values

        k = (end_ts - values.index[-1]) // self._epoch_length
        return self.read_k_upcoming(k, metric_ids)

    # Both ends inclusive
    def read_between_times(
        self,
        start_time: datetime,
        end_time: datetime,
        metric_ids: List[str] | None = None,
    ) -> pd.DataFrame:
        values = self._metrics_values()
        if values.empty:
            return values

        past = values.loc[(values.index >= start_time) & (values.index <= end_time)]
        future = self.read_upcoming_until(end_time, metric_ids)

        return pd.concat([past, future], axis=0)

    # Both ends inclusive
    def read_between_epochs(self, start_epoch: int, end_epoch: int) -> pd.DataFrame:
        values = self._metrics_values()
        if values.empty:
            return values

        past = self.read_k_most_recent(max(0, -start_epoch)).head(
            end_epoch - start_epoch + 1
        )
        future = self.read_k_upcoming(max(0, end_epoch + 1)).tail(
            end_epoch - start_epoch + 1
        )

        return pd.concat([past, future], axis=0)