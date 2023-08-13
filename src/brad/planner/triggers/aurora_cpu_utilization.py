from typing import Optional

from .metrics_thresholds import MetricsThresholds
from .trigger import Trigger
from brad.daemon.monitor import Monitor


class AuroraCpuUtilization(Trigger):
    def __init__(
        self,
        monitor: Monitor,
        lo: float,
        hi: float,
        sustained_epochs: int = 1,
        lookahead_epochs: Optional[int] = None,
    ) -> None:
        super().__init__()
        self._monitor = monitor
        self._impl = MetricsThresholds(lo, hi, sustained_epochs)
        self._sustained_epochs = sustained_epochs
        self._lookahead_epochs = lookahead_epochs

    def should_replan(self) -> bool:
        # TODO: May want to consider read replica metrics too.
        past = self._monitor.aurora_metrics(reader_index=None).read_k_most_recent(
            k=self._sustained_epochs, metric_ids=[_UTILIZATION_METRIC]
        )
        if self._impl.exceeds_thresholds(
            past[_UTILIZATION_METRIC], "Aurora CPU utilization"
        ):
            return True

        if self._lookahead_epochs is None:
            return False

        future = self._monitor.aurora_metrics(reader_index=None).read_k_upcoming(
            k=self._lookahead_epochs, metric_ids=[_UTILIZATION_METRIC]
        )
        return self._impl.exceeds_thresholds(
            future[_UTILIZATION_METRIC], "forecasted Aurora CPU Utilization"
        )


_UTILIZATION_METRIC = "os.cpuUtilization.total.avg"
