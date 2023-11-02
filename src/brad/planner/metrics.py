import logging
import math
import pytz
import pandas as pd
import numpy as np
from datetime import datetime
from collections import namedtuple
from typing import Tuple, Optional

from brad.blueprint.manager import BlueprintManager
from brad.config.metrics import FrontEndMetric
from brad.daemon.monitor import Monitor

logger = logging.getLogger(__name__)

Metrics = namedtuple(
    "Metrics",
    [
        "redshift_cpu_avg",
        "aurora_writer_cpu_avg",
        "aurora_reader_cpu_avg",
        "aurora_writer_buffer_hit_pct_avg",
        "aurora_reader_buffer_hit_pct_avg",
        "aurora_writer_load_minute_avg",
        "aurora_reader_load_minute_avg",
        "txn_completions_per_s",
        "txn_lat_s_p50",
        "txn_lat_s_p90",
    ],
)


class MetricsProvider:
    """
    An abstract interface over a component that can provide metrics (for
    blueprint planning purposes).
    """

    def get_metrics(self) -> Tuple[Metrics, datetime]:
        """
        Return the metrics and the timestamp associated with them (when the
        metrics were recorded).
        """
        raise NotImplementedError


class FixedMetricsProvider(MetricsProvider):
    """
    Always returns the same fixed metrics. Used for debugging purposes.
    """

    def __init__(self, metrics: Metrics, timestamp: datetime) -> None:
        self._metrics = metrics
        self._timestamp = timestamp

    def get_metrics(self) -> Tuple[Metrics, datetime]:
        return (self._metrics, self._timestamp)


class MetricsFromMonitor(MetricsProvider):
    def __init__(self, monitor: Monitor, blueprint_mgr: BlueprintManager) -> None:
        self._monitor = monitor
        self._blueprint_mgr = blueprint_mgr

    def get_metrics(self) -> Tuple[Metrics, datetime]:
        blueprint = self._blueprint_mgr.get_blueprint()
        aurora_on = blueprint.aurora_provisioning().num_nodes() > 0
        redshift_on = blueprint.redshift_provisioning().num_nodes() > 0
        aurora_has_readers = blueprint.aurora_provisioning().num_nodes() > 1

        redshift_source = self._monitor.redshift_metrics()
        aurora_writer_source = self._monitor.aurora_writer_metrics()
        front_end_source = self._monitor.front_end_metrics()

        # The `max()` of `real_time_delay()` indicates the number of previous
        # epochs where a metrics source's metrics can be unavailable. We add one
        # to get the minimum number of epochs we should read to be able to find
        # an intersection across all metrics sources.
        max_available_after_epochs = (
            max(
                redshift_source.real_time_delay(),
                aurora_writer_source.real_time_delay(),
                front_end_source.real_time_delay(),
            )
            + 1
        )

        # TODO: Need to support forecasted metrics.
        # TODO: We should extract all metric values over the planning window and
        # provide them to the downstream consumer (e.g., the planner).
        redshift = (
            redshift_source.read_k_most_recent(
                k=max_available_after_epochs, metric_ids=_REDSHIFT_METRICS
            )
            if redshift_on
            else pd.DataFrame([], columns=_REDSHIFT_METRICS)
        )
        aurora_writer = (
            aurora_writer_source.read_k_most_recent(
                k=max_available_after_epochs + 1, metric_ids=_AURORA_METRICS
            )
            if aurora_on
            else pd.DataFrame([], columns=_AURORA_METRICS)
        )
        if aurora_has_readers:
            reader_metrics = []
            for aurora_reader_source in self._monitor.aurora_reader_metrics():
                reader_metrics.append(
                    aurora_reader_source.read_k_most_recent(
                        k=max_available_after_epochs + 1, metric_ids=_AURORA_METRICS
                    )
                )
            combined = pd.concat(reader_metrics)
            # We take the mean across all read replicas (assume load is equally
            # split across replicas).
            aurora_reader = combined.groupby(combined.index).mean()
        else:
            aurora_reader = pd.DataFrame([], columns=_AURORA_METRICS)
        front_end = front_end_source.read_k_most_recent(
            k=max_available_after_epochs, metric_ids=_FRONT_END_METRICS
        )

        if redshift.empty and aurora_writer.empty and front_end.empty:
            logger.warning("All metrics are empty.")
            now = datetime.now().astimezone(pytz.utc)
            return (Metrics(1.0, 1.0, 1.0, 100.0, 100.0, 1.0, 1.0, 1.0, 0.0, 0.0), now)

        assert not front_end.empty, "Front end metrics are empty."

        # These metrics may be empty if the engine is "off" (paused).
        # We fill them with 0s just to simplify the rest of the code. The
        # metrics of "turned off" engines are not used during the rest of the
        # planning process.
        if redshift.empty:
            redshift = self._fill_empty_metrics(redshift, front_end)
        if aurora_writer.empty:
            aurora_writer = self._fill_empty_metrics(aurora_writer, front_end)
        if aurora_reader.empty:
            aurora_reader = self._fill_empty_metrics(aurora_reader, front_end)

        # Align timestamps across the metrics.
        common_timestamps = (
            front_end.index.intersection(aurora_writer.index)
            .intersection(redshift.index)
            .intersection(aurora_reader.index)
        )
        if len(common_timestamps) == 0:
            most_recent_common = front_end.index.max()
            logger.warning(
                "Metrics timestamp intersection is empty. Falling back to the front-end timestamps: %s",
                str(most_recent_common),
            )
        else:
            most_recent_common = common_timestamps.max()
        logger.debug(
            "MetricsFromMonitor using metrics starting at %s", str(most_recent_common)
        )

        redshift_cpu = self._extract_most_recent_possibly_missing(
            redshift.loc[redshift.index <= most_recent_common, _REDSHIFT_METRICS[0]],
            default_value=0.0,
            name="redshift_cpu",
        )
        txn_per_s = self._extract_most_recent_possibly_missing(
            front_end.loc[front_end.index <= most_recent_common, _FRONT_END_METRICS[0]],
            default_value=0.0,
            name="txn_per_s",
        )
        txn_lat_s_p50 = self._extract_most_recent_possibly_missing(
            front_end.loc[
                front_end.index <= most_recent_common,
                FrontEndMetric.TxnLatencySecondP50.value,
            ],
            default_value=0.0,
            name="txn_lat_s_p50",
        )
        txn_lat_s_p90 = self._extract_most_recent_possibly_missing(
            front_end.loc[
                front_end.index <= most_recent_common,
                FrontEndMetric.TxnLatencySecondP90.value,
            ],
            default_value=0.0,
            name="txn_lat_s_p90",
        )

        aurora_writer_rel = aurora_writer.loc[aurora_writer.index <= most_recent_common]
        aurora_reader_rel = aurora_reader.loc[aurora_reader.index <= most_recent_common]

        aurora_writer_cpu = self._extract_most_recent_possibly_missing(
            aurora_writer_rel[_AURORA_METRICS[0]],
            default_value=0.0,
            name="aurora_writer_cpu",
        )
        aurora_reader_cpu = self._extract_most_recent_possibly_missing(
            aurora_reader_rel[_AURORA_METRICS[0]],
            default_value=0.0,
            name="aurora_reader_cpu",
        )

        aurora_writer_load_minute = self._recover_load_value(
            aurora_writer_rel, "aurora_writer_load_minute"
        )
        aurora_reader_load_minute = self._recover_load_value(
            aurora_reader_rel, "aurora_reader_load_minute"
        )

        aurora_writer_hit_rate_pct = self._extract_most_recent_possibly_missing(
            aurora_writer_rel[_AURORA_METRICS[2]],
            default_value=0.0,
            name="aurora_writer_hit_rate_pct",
        )
        aurora_reader_hit_rate_pct = self._extract_most_recent_possibly_missing(
            aurora_reader_rel[_AURORA_METRICS[2]],
            default_value=0.0,
            name="aurora_reader_hit_rate_pct",
        )

        return (
            Metrics(
                redshift_cpu_avg=redshift_cpu,
                aurora_writer_cpu_avg=aurora_writer_cpu,
                aurora_reader_cpu_avg=aurora_reader_cpu,
                aurora_writer_buffer_hit_pct_avg=aurora_writer_hit_rate_pct,
                aurora_reader_buffer_hit_pct_avg=aurora_reader_hit_rate_pct,
                aurora_writer_load_minute_avg=aurora_writer_load_minute,
                aurora_reader_load_minute_avg=aurora_reader_load_minute,
                txn_completions_per_s=txn_per_s,
                txn_lat_s_p50=txn_lat_s_p50,
                txn_lat_s_p90=txn_lat_s_p90,
            ),
            most_recent_common.to_pydatetime(),
        )

    def _fill_empty_metrics(
        self, to_fill: pd.DataFrame, guide: pd.DataFrame
    ) -> pd.DataFrame:
        num_rows = guide.shape[0]
        num_cols = len(to_fill.columns)
        return pd.DataFrame(
            np.zeros((num_rows, num_cols)), columns=to_fill.columns, index=guide.index
        )

    def _extract_most_recent_possibly_missing(
        self, series: pd.Series, default_value: int | float, name: Optional[str] = None
    ) -> int | float:
        if name is not None and len(series) == 0:
            logger.warning(
                "Using default metric value %s for %s", str(default_value), name
            )

        if len(series) == 0:
            return default_value
        else:
            return series.iloc[-1]

    def _recover_load_value(self, aurora_rel: pd.DataFrame, metric_name: str) -> float:
        if len(aurora_rel) < 2:
            logger.warning("Not enough Aurora metric entries to compute current load.")
            load_minute = self._extract_most_recent_possibly_missing(
                aurora_rel[_AURORA_METRICS[1]],
                default_value=0.0,
                name=metric_name,
            )
        else:
            # Load averages are exponentially averaged. We do the following to
            # recover the load value for the last minute.
            exp_1 = math.exp(-1)
            exp_1_rest = 1 - exp_1
            load_last = aurora_rel[_AURORA_METRICS[1]].iloc[-1]
            load_2nd_last = aurora_rel[_AURORA_METRICS[1]].iloc[-2]
            load_minute = (load_last - exp_1 * load_2nd_last) / exp_1_rest
            load_minute = max(0.0, load_minute)  # To avoid negative loads.
            logger.debug(
                "Aurora load renormalization: %.4f, %.4f, %.4f",
                load_2nd_last,
                load_last,
                load_minute,
            )
        return load_minute


_AURORA_METRICS = [
    "os.cpuUtilization.total.avg",
    "os.loadAverageMinute.one.avg",
    "BufferCacheHitRatio_Average",
]

_REDSHIFT_METRICS = [
    "CPUUtilization_Average",
]

_FRONT_END_METRICS = [
    FrontEndMetric.TxnEndPerSecond.value,
    FrontEndMetric.TxnLatencySecondP50.value,
    FrontEndMetric.TxnLatencySecondP90.value,
]
