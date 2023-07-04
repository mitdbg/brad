from collections import namedtuple
from brad.daemon.monitor import Monitor

Metrics = namedtuple(
    "Metrics",
    [
        "redshift_cpu_avg",
        "aurora_cpu_avg",
        "buffer_hit_pct_avg",
        "aurora_load_minute_avg",
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
            metrics = self._monitor.read_k_upcoming(
                k=1, metric_ids=list(_RELEVANT_METRICS.values())
            )
        else:
            metrics = self._monitor.read_k_most_recent(
                k=1, metric_ids=list(_RELEVANT_METRICS.values())
            )

        if metrics.empty:
            return Metrics(1.0, 1.0, 100.0, 1.0)

        redshift_cpu = metrics[_RELEVANT_METRICS["redshift_cpu_avg"]].iloc[0]
        aurora_cpu = metrics[_RELEVANT_METRICS["aurora_cpu_avg"]].iloc[0]
        hit_pct = metrics[_RELEVANT_METRICS["buffer_hit_pct_avg"]].iloc[0]

        # TODO: Retrieve performance insights metrics.
        return Metrics(redshift_cpu, aurora_cpu, hit_pct, aurora_load_minute_avg=1.0)


_RELEVANT_METRICS = {
    "redshift_cpu_avg": "redshift_CPUUtilization_Average",
    "aurora_cpu_avg": "aurora_WRITER_CPUUtilization_Average",
    "buffer_hit_pct_avg": "aurora_WRITER_BufferCacheHitRatio_Average",
}
