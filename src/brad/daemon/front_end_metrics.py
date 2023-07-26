import math
import pandas as pd
import pytz
from typing import Dict, List
from datetime import datetime, timezone

from .metrics_source import MetricsSourceWithForecasting
from brad.config.file import ConfigFile
from brad.config.metrics import FrontEndMetric
from brad.daemon.messages import MetricsReport
from brad.utils.streaming_metric import StreamingMetric


FrontEndMetricDict = Dict[FrontEndMetric, List[StreamingMetric]]


class FrontEndMetrics(MetricsSourceWithForecasting):
    def __init__(
        self,
        config: ConfigFile,
        forecasting_method: str,
        forecasting_window_size: int,
    ) -> None:
        self._config = config
        self._epoch_length = self._config.epoch_length
        samples_per_epoch = (
            self._epoch_length.total_seconds()
            / self._config.front_end_metrics_reporting_period_seconds
        )
        sm_window_size = math.ceil(10 * samples_per_epoch)
        self._front_end_metrics: FrontEndMetricDict = {
            FrontEndMetric.TxnEndPerSecond: [
                StreamingMetric[float](sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
        }
        self._ordered_metrics = list(self._front_end_metrics.keys())
        self._values_df = pd.DataFrame(columns=list(map(str, self._ordered_metrics)))

        super().__init__(
            self._epoch_length, forecasting_method, forecasting_window_size
        )

    async def fetch_latest(self) -> None:
        now = datetime.now(tz=timezone.utc)
        num_epochs = 5
        end_time = (
            now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % self._epoch_length
        )
        start_time = end_time - num_epochs * self._epoch_length

        timestamps = []
        data_cols: Dict[str, List[float]] = {
            metric_kind.value: [] for metric_kind in self._ordered_metrics
        }

        for offset in range(num_epochs):
            window_start = start_time + offset * self._epoch_length
            window_end = window_start + self._epoch_length
            for metric, values in self._front_end_metrics.items():
                total = sum(
                    map(
                        # pylint: disable-next=cell-var-from-loop
                        lambda val: val.average_in_window(window_start, window_end),
                        values,
                    )
                )
                data_cols[metric.value].append(total)
            timestamps.append(window_end)

        # Sanity checks.
        assert len(timestamps) == data_cols[FrontEndMetric.TxnEndPerSecond.value]

        new_metrics = pd.DataFrame(data_cols, index=timestamps)
        self._values_df = (
            self._values_df
            if self._values_df.empty
            else pd.concat(
                [
                    self._values_df,
                    new_metrics.loc[new_metrics.index > self._values_df.index[-1]],
                ]
            )
        )
        await super().fetch_latest()

    def _metrics_values(self) -> pd.DataFrame:
        return self._values_df

    def handle_metric_report(self, report: MetricsReport) -> None:
        now = datetime.now(tz=timezone.utc)
        # Each front end server reports this metric.
        metric = self._front_end_metrics[FrontEndMetric.TxnEndPerSecond][
            report.fe_index
        ]
        metric.add_sample(report.txn_completions_per_s, now)
