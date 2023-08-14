import logging
from typing import Optional

from .metrics_thresholds import MetricsThresholds
from .trigger import Trigger
from brad.daemon.monitor import Monitor

logger = logging.getLogger(__name__)


class RedshiftCpuUtilization(Trigger):
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

    async def should_replan(self) -> bool:
        if self._current_blueprint is None:
            logger.info(
                "Redshift CPU utilization not running because of missing blueprint."
            )
            return False

        if self._current_blueprint.redshift_provisioning().num_nodes() == 0:
            logger.debug(
                "Redshift is off, so the Redshift CPU utilization trigger is inactive."
            )
            return False

        past = self._monitor.redshift_metrics().read_k_most_recent(
            k=self._sustained_epochs, metric_ids=[_UTILIZATION_METRIC]
        )
        if self._impl.exceeds_thresholds(
            past[_UTILIZATION_METRIC], "Redshift CPU utilization"
        ):
            return True

        if self._lookahead_epochs is None:
            return False

        future = self._monitor.redshift_metrics().read_k_upcoming(
            k=self._lookahead_epochs, metric_ids=[_UTILIZATION_METRIC]
        )
        return self._impl.exceeds_thresholds(
            future[_UTILIZATION_METRIC], "forecasted Redshift CPU Utilization"
        )


_UTILIZATION_METRIC = "CPUUtilization_Average"
