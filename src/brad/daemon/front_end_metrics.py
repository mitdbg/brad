import math
import logging
import pandas as pd
import pytz
from typing import Dict, List, Optional
from datetime import datetime, timezone

from .metrics_source import MetricsSourceWithForecasting
from brad.config.file import ConfigFile
from brad.config.metrics import FrontEndMetric
from brad.daemon.messages import MetricsReport
from brad.daemon.metrics_logger import MetricsLogger
from brad.utils.streaming_metric import StreamingMetric

logger = logging.getLogger(__name__)


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
            FrontEndMetric.QueryLatencySumSecond: [
                StreamingMetric[float](sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
            FrontEndMetric.NumQueries: [
                StreamingMetric[float](sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
            FrontEndMetric.QueryLatencyMaxSecond: [
                StreamingMetric[float](sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
        }
        self._ordered_metrics = list(self._front_end_metrics.keys())
        self._values_df = pd.DataFrame(
            columns=list(map(lambda metric: metric.value, self._ordered_metrics))
        )
        self._logger = MetricsLogger.create_from_config(
            self._config, "brad_metrics_front_end.log"
        )

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
                if (
                    metric == FrontEndMetric.TxnEndPerSecond
                    or metric == FrontEndMetric.QueryLatencySumSecond
                    or metric == FrontEndMetric.NumQueries
                ):
                    total = sum(
                        map(
                            # pylint: disable-next=cell-var-from-loop
                            lambda val: val.average_in_window(window_start, window_end),
                            values,
                        )
                    )
                    data_cols[metric.value].append(total)
                elif metric == FrontEndMetric.QueryLatencyMaxSecond:
                    max_val = max(
                        map(
                            # pylint: disable-next=cell-var-from-loop
                            lambda val: val.max_in_window(window_start, window_end),
                            values,
                        )
                    )
                    data_cols[metric.value].append(max_val)
                else:
                    logger.warning("Unhandled front end metric: %s", metric)
                    data_cols[metric.value].append(0.0)
            timestamps.append(window_end)

        # Sanity checks.
        assert len(timestamps) == len(data_cols[FrontEndMetric.TxnEndPerSecond.value])

        new_metrics = pd.DataFrame(data_cols, index=timestamps)
        self._values_df = self._get_updated_metrics(new_metrics)
        await super().fetch_latest()

    def _metrics_values(self) -> pd.DataFrame:
        return self._values_df

    def _metrics_logger(self) -> Optional[MetricsLogger]:
        return self._logger

    def handle_metric_report(self, report: MetricsReport) -> None:
        now = datetime.now(tz=timezone.utc)
        fe_index = report.fe_index

        # Each front end server reports these metrics.
        txns = self._front_end_metrics[FrontEndMetric.TxnEndPerSecond][fe_index]
        txns.add_sample(report.txn_completions_per_s, now)

        query_lat = self._front_end_metrics[FrontEndMetric.QueryLatencySumSecond][
            fe_index
        ]
        query_lat.add_sample(report.latency.sum, now)

        query_count = self._front_end_metrics[FrontEndMetric.NumQueries][fe_index]
        query_count.add_sample(report.latency.num_values, now)

        query_lat_max = self._front_end_metrics[FrontEndMetric.QueryLatencyMaxSecond][
            fe_index
        ]
        query_lat_max.add_sample(max(report.latency.top_k), now)

        logger.debug(
            "Received metrics report: [%d] %f (ts: %s)",
            report.fe_index,
            report.txn_completions_per_s,
            now,
        )
