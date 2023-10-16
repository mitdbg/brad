import enum
import math
import logging
import pandas as pd
import pytz
from typing import Dict, List, Optional
from datetime import datetime, timezone
from ddsketch import DDSketch

from .metrics_source import MetricsSourceWithForecasting
from brad.config.file import ConfigFile
from brad.config.metrics import FrontEndMetric
from brad.daemon.messages import MetricsReport
from brad.daemon.metrics_logger import MetricsLogger
from brad.utils.streaming_metric import StreamingMetric, StreamingNumericMetric

logger = logging.getLogger(__name__)


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
        self._numeric_front_end_metrics: Dict[
            _MetricKey, List[StreamingNumericMetric]
        ] = {
            _MetricKey.TxnEndPerSecond: [
                StreamingNumericMetric(sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
        }
        self._sketch_front_end_metrics: Dict[
            _MetricKey, List[StreamingMetric[DDSketch]]
        ] = {
            _MetricKey.QueryLatencySecond: [
                StreamingMetric[DDSketch](sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
            _MetricKey.TxnLatencySecond: [
                StreamingMetric[DDSketch](sm_window_size)
                for _ in range(self._config.num_front_ends)
            ],
        }
        self._ordered_metrics: List[str] = [
            FrontEndMetric.TxnEndPerSecond.value,
            FrontEndMetric.QueryLatencySecondP50.value,
            FrontEndMetric.TxnLatencySecondP50.value,
            FrontEndMetric.QueryLatencySecondP90.value,
            FrontEndMetric.TxnLatencySecondP90.value,
        ]
        self._values_df = pd.DataFrame(columns=self._ordered_metrics.copy())
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
            metric_name: [] for metric_name in self._ordered_metrics
        }

        for offset in range(num_epochs):
            window_start = start_time + offset * self._epoch_length
            window_end = window_start + self._epoch_length
            for metric_key, values in self._numeric_front_end_metrics.items():
                if metric_key == _MetricKey.TxnEndPerSecond:
                    total = sum(
                        map(
                            # pylint: disable-next=cell-var-from-loop
                            lambda val: val.average_in_window(window_start, window_end),
                            values,
                        )
                    )
                    data_cols[FrontEndMetric.TxnEndPerSecond.value].append(total)
                else:
                    logger.warning("Unhandled front end metric: %s", metric_key)

            for metric_key, fe_sketches in self._sketch_front_end_metrics.items():
                if (
                    metric_key == _MetricKey.QueryLatencySecond
                    or metric_key == _MetricKey.TxnLatencySecond
                ):
                    merged = None
                    for sketches in fe_sketches:
                        for sketch, _ in sketches.window_iterator(
                            window_start, window_end
                        ):
                            if merged is None:
                                merged = sketch
                            else:
                                merged = merged.merge(sketch)

                    if merged is None:
                        logger.warning(
                            "Missing latency sketch values for %s", metric_key
                        )
                        p50_val = 0.0
                        p90_val = 0.0
                    else:
                        p50_val_cand = merged.get_quantile_value(0.5)
                        p90_val_cand = merged.get_quantile_value(0.9)
                        p50_val = p50_val_cand if p50_val_cand is not None else 0.0
                        p90_val = p90_val_cand if p90_val_cand is not None else 0.0

                    if metric_key == _MetricKey.QueryLatencySecond:
                        data_cols[FrontEndMetric.QueryLatencySecondP50.value].append(
                            p50_val
                        )
                        data_cols[FrontEndMetric.QueryLatencySecondP90.value].append(
                            p90_val
                        )
                    else:
                        data_cols[FrontEndMetric.TxnLatencySecondP50.value].append(
                            p50_val
                        )
                        data_cols[FrontEndMetric.TxnLatencySecondP90.value].append(
                            p90_val
                        )
                else:
                    logger.warning("Unhandled front end metric: %s", metric_key)

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
        self._numeric_front_end_metrics[_MetricKey.TxnEndPerSecond][
            fe_index
        ].add_sample(report.txn_completions_per_s, now)
        self._sketch_front_end_metrics[_MetricKey.QueryLatencySecond][
            fe_index
        ].add_sample(report.query_latency_sketch(), now)
        self._sketch_front_end_metrics[_MetricKey.TxnLatencySecond][
            fe_index
        ].add_sample(report.txn_latency_sketch(), now)

        logger.debug(
            "Received metrics report: [%d] %f (ts: %s)",
            report.fe_index,
            report.txn_completions_per_s,
            now,
        )


class _MetricKey(enum.Enum):
    TxnEndPerSecond = "txn_end_per_s"
    QueryLatencySecond = "query_latency_s"
    TxnLatencySecond = "txn_latency_s"
