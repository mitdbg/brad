import math
import logging
import pandas as pd
import pytz
import copy
from typing import Dict, List, Optional
from datetime import datetime
from ddsketch import DDSketch

from .metrics_source import MetricsSourceWithForecasting
from brad.config.file import ConfigFile
from brad.daemon.messages import VdbeMetricsReport
from brad.daemon.metrics_logger import MetricsLogger
from brad.utils.streaming_metric import StreamingMetric
from brad.utils import log_verbose
from brad.utils.time_periods import universal_now

logger = logging.getLogger(__name__)


class VdbeMetrics(MetricsSourceWithForecasting):
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
        self._sm_window_size = math.ceil(200 * samples_per_epoch)

        # (vdbe_id -> metric)
        self._sketch_front_end_metrics: Dict[int, StreamingMetric[DDSketch]] = {}
        # All known VDBE IDs.
        self._ordered_metrics: List[int] = []
        self._values_df = pd.DataFrame([])
        self._logger = MetricsLogger.create_from_config(
            self._config, "brad_vdbe_metrics_front_end.log"
        )

        super().__init__(
            self._epoch_length, forecasting_method, forecasting_window_size
        )

    async def fetch_latest(self) -> None:
        now = universal_now()
        num_epochs = 5
        end_time = (
            now - (now - datetime.min.replace(tzinfo=pytz.UTC)) % self._epoch_length
        )
        start_time = end_time - num_epochs * self._epoch_length

        timestamps = []
        data_cols: Dict[str, List[float]] = {
            str(metric_name): [] for metric_name in self._ordered_metrics
        }

        for offset in range(num_epochs):
            window_start = start_time + offset * self._epoch_length
            window_end = window_start + self._epoch_length

            logger.debug(
                "Loading front end metrics for %s -- %s", window_start, window_end
            )

            for vdbe_id, sketches in self._sketch_front_end_metrics.items():
                merged = None
                num_matching = 0
                min_ts = None
                max_ts = None

                for sketch, ts in sketches.window_iterator(window_start, window_end):
                    # These stats are for debug logging.
                    num_matching += 1
                    if min_ts is not None:
                        min_ts = min(min_ts, ts)
                    else:
                        min_ts = ts
                    if max_ts is not None:
                        max_ts = max(max_ts, ts)
                    else:
                        max_ts = ts

                    if merged is not None:
                        merged.merge(sketch)
                    else:
                        # DDSketch.merge() is an inplace method. We want
                        # to avoid modifying the stored sketches so we
                        # make a copy.
                        merged = copy.deepcopy(sketch)

                if merged is None:
                    logger.warning("Missing latency sketch values for VDBE %d", vdbe_id)
                    p90_val = 0.0
                else:
                    p90_val_cand = merged.get_quantile_value(0.9)
                    p90_val = p90_val_cand if p90_val_cand is not None else 0.0

                data_cols[str(vdbe_id)].append(p90_val)

            timestamps.append(window_end)

        new_metrics = pd.DataFrame(data_cols, index=timestamps)
        self._values_df = self._get_updated_metrics(new_metrics)
        await super().fetch_latest()

    def _metrics_values(self) -> pd.DataFrame:
        return self._values_df

    def _metrics_logger(self) -> Optional[MetricsLogger]:
        return self._logger

    def handle_metric_report(self, report: VdbeMetricsReport) -> None:
        now = universal_now()
        logger.debug("Handling VDBE metrics report: (ts: %s)", now)
        for vdbe_id, sketch in report.query_latency_sketches():
            p90 = sketch.get_quantile_value(0.9)
            logger.debug("Has sketch for VDBE %d. p90: %f", vdbe_id, p90)

        for vdbe_id, sketch in report.query_latency_sketches():
            if vdbe_id not in self._sketch_front_end_metrics:
                self._sketch_front_end_metrics[vdbe_id] = StreamingMetric(
                    self._sm_window_size
                )
                self._ordered_metrics.append(vdbe_id)
            self._sketch_front_end_metrics[vdbe_id].add_sample(sketch, now)

        log_verbose(
            logger,
            "Received VDBE metrics report: (ts: %s)",
            now,
        )
