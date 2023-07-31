import logging
import math
import pytz
from datetime import datetime
from collections import namedtuple
from typing import Tuple

from brad.config.metrics import FrontEndMetric
from brad.daemon.monitor import Monitor

logger = logging.getLogger(__name__)

Metrics = namedtuple(
    "Metrics",
    [
        "redshift_cpu_avg",
        "aurora_cpu_avg",
        "buffer_hit_pct_avg",
        "aurora_load_minute_avg",
        "txn_completions_per_s",
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
    def __init__(self, monitor: Monitor) -> None:
        self._monitor = monitor

    def get_metrics(self) -> Tuple[Metrics, datetime]:
        redshift_source = self._monitor.redshift_metrics()
        aurora_source = self._monitor.aurora_metrics(reader_index=None)
        front_end_source = self._monitor.front_end_metrics()

        # The `max()` of `real_time_delay()` indicates the number of previous
        # epochs where a metrics source's metrics can be unavailable. We add one
        # to get the minimum number of epochs we should read to be able to find
        # an intersection across all metrics sources.
        max_available_after_epochs = (
            max(
                redshift_source.real_time_delay(),
                aurora_source.real_time_delay(),
                front_end_source.real_time_delay(),
            )
            + 1
        )

        # TODO: Need to support forecasted metrics.
        redshift = redshift_source.read_k_most_recent(
            k=max_available_after_epochs, metric_ids=_REDSHIFT_METRICS
        )
        aurora = aurora_source.read_k_most_recent(
            k=max_available_after_epochs + 1, metric_ids=_AURORA_METRICS
        )
        front_end = front_end_source.read_k_most_recent(
            k=max_available_after_epochs, metric_ids=_FRONT_END_METRICS
        )

        if redshift.empty or aurora.empty or front_end.empty:
            logger.warning(
                "Empty metrics. Redshift empty: %r, Aurora empty: %r, Front end empty: %r",
                redshift.empty,
                aurora.empty,
                front_end.empty,
            )
            now = datetime.now().astimezone(pytz.utc)
            return (Metrics(1.0, 1.0, 100.0, 1.0, 1.0), now)

        # Align timestamps across the metrics.
        common_timestamps = redshift.index.intersection(aurora.index).intersection(
            front_end.index
        )
        most_recent_common = common_timestamps.max()
        logger.debug(
            "MetricsFromMonitor using metrics starting at %s", str(most_recent_common)
        )

        redshift_cpu = redshift.loc[
            redshift.index <= most_recent_common, _REDSHIFT_METRICS[0]
        ].iloc[-1]
        txn_per_s = front_end.loc[
            front_end.index <= most_recent_common, _FRONT_END_METRICS[0]
        ].iloc[-1]

        aurora_rel = aurora.loc[aurora.index <= most_recent_common]
        aurora_cpu = aurora_rel[_AURORA_METRICS[0]].iloc[-1]

        if len(aurora_rel) < 2:
            logger.warning("Not enough Aurora metric entries to compute current load.")
            load_minute = aurora_rel[_AURORA_METRICS[1]].iloc[-1]
        else:
            # Load averages are exponentially averaged. We do the following to
            # recover the load value for the last minute.
            exp_1 = math.exp(-1)
            exp_1_rest = 1 - exp_1
            load_last = aurora_rel[_AURORA_METRICS[1]].iloc[-1]
            load_2nd_last = aurora_rel[_AURORA_METRICS[1]].iloc[-2]
            load_minute = (load_last - exp_1 * load_2nd_last) / exp_1_rest

        blks_read = aurora_rel[_AURORA_METRICS[2]].iloc[-1]
        blks_hit = aurora_rel[_AURORA_METRICS[3]].iloc[-1]
        hit_rate = blks_hit / (blks_read + blks_hit)

        return (
            Metrics(
                redshift_cpu,
                aurora_cpu,
                buffer_hit_pct_avg=hit_rate * 100,
                aurora_load_minute_avg=load_minute,
                txn_completions_per_s=txn_per_s,
            ),
            most_recent_common,
        )


_AURORA_METRICS = [
    "os.cpuUtilization.total.avg",
    "os.loadAverageMinute.one.avg",
    "db.IO.blks_read.avg",
    "db.Cache.blks_hit.avg",
]

_REDSHIFT_METRICS = [
    "CPUUtilization_Average",
]

_FRONT_END_METRICS = [
    FrontEndMetric.TxnEndPerSecond.value,
]
