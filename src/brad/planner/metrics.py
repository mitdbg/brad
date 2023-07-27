import logging
import math
from collections import namedtuple

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
        "client_txn_completions_per_s_avg",
    ],
)


class MetricsProvider:
    """
    An abstract interface over a component that can provide metrics (for
    blueprint planning purposes).
    """

    def get_metrics(self) -> Metrics:
        raise NotImplementedError


class FixedMetricsProvider(MetricsProvider):
    """
    Always returns the same fixed metrics. Used for debugging purposes.
    """

    def __init__(self, metrics: Metrics) -> None:
        self._metrics = metrics

    def get_metrics(self) -> Metrics:
        return self._metrics


class MetricsFromMonitor(MetricsProvider):
    def __init__(self, monitor: Monitor, forecasted: bool = True) -> None:
        self._monitor = monitor
        self._forecasted = forecasted

    def get_metrics(self) -> Metrics:
        if self._forecasted:
            redshift_source = self._monitor.redshift_metrics().read_k_upcoming
            aurora_source = self._monitor.aurora_metrics(
                reader_index=None
            ).read_k_upcoming
            front_end_source = self._monitor.front_end_metrics().read_k_upcoming
        else:
            redshift_source = self._monitor.redshift_metrics().read_k_most_recent
            aurora_source = self._monitor.aurora_metrics(
                reader_index=None
            ).read_k_most_recent
            front_end_source = self._monitor.front_end_metrics().read_k_most_recent

        redshift = redshift_source(k=1, metric_ids=_REDSHIFT_METRICS)
        aurora = aurora_source(k=2, metric_ids=_AURORA_METRICS)
        front_end = front_end_source(k=1, metric_ids=_FRONT_END_METRICS)

        if redshift.empty or aurora.empty or front_end.empty:
            logger.warning(
                "Empty metrics. Redshift empty: %r, Aurora empty: %r, Front end empty: %r",
                redshift.empty,
                aurora.empty,
                front_end.empty,
            )
            return Metrics(1.0, 1.0, 100.0, 1.0, 1.0)

        redshift_cpu = redshift[_REDSHIFT_METRICS[0]].iloc[0]
        aurora_cpu = aurora[_AURORA_METRICS[0]].iloc[1]

        # Load averages are exponentially averaged. We do the following to
        # recover the load value for the last minute.
        exp_1 = math.exp(-1)
        exp_1_rest = 1 - exp_1
        load_last = aurora[_AURORA_METRICS[1]].iloc[1]
        load_2nd_last = aurora[_AURORA_METRICS[1]].iloc[0]
        load_minute = (load_last - exp_1 * load_2nd_last) / exp_1_rest

        blks_read = aurora[_AURORA_METRICS[2]].iloc[1]
        blks_hit = aurora[_AURORA_METRICS[3]].iloc[1]
        hit_rate = blks_hit / (blks_read + blks_hit)

        completions = front_end[_FRONT_END_METRICS[0]].iloc[0]

        return Metrics(
            redshift_cpu,
            aurora_cpu,
            buffer_hit_pct_avg=hit_rate * 100,
            aurora_load_minute_avg=load_minute,
            client_txn_completions_per_s_avg=completions,
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
