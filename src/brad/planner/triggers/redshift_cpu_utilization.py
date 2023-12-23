import logging
from datetime import timedelta
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
        epoch_length: timedelta,
        observe_bp_delay: timedelta,
        sustained_epochs: int = 1,
        lookahead_epochs: Optional[int] = None,
    ) -> None:
        super().__init__(epoch_length, observe_bp_delay)
        self._monitor = monitor
        self._impl = MetricsThresholds(lo, hi, sustained_epochs)
        self._sustained_epochs = sustained_epochs
        self._lookahead_epochs = lookahead_epochs

    async def should_replan(self) -> bool:
        if self._current_blueprint is None:
            logger.info(
                "Redshift CPU utilization trigger not running because of missing blueprint."
            )
            return False

        if self._current_blueprint.redshift_provisioning().num_nodes() == 0:
            logger.debug(
                "Redshift is off, so the Redshift CPU utilization trigger is inactive."
            )
            return False

        if not self._passed_delays_since_cutoff():
            logger.debug(
                "Skippping Redshift CPU utilization trigger because we have not passed the delay cutoff."
            )
            return False

        past = self._monitor.redshift_metrics().read_k_most_recent(
            k=self._sustained_epochs, metric_ids=[_UTILIZATION_METRIC]
        )
        relevant = past[past.index > self._cutoff]
        if self._impl.exceeds_thresholds(
            relevant[_UTILIZATION_METRIC], "Redshift CPU utilization"
        ):
            return True

        if self._lookahead_epochs is None:
            return False

        if not self._passed_n_epochs_since_cutoff(self._sustained_epochs):
            # We do not trigger based on a forecast if `sustained_epochs` has
            # not passed since the last cutoff.
            return False

        future = self._monitor.redshift_metrics().read_k_upcoming(
            k=self._lookahead_epochs, metric_ids=[_UTILIZATION_METRIC]
        )
        return self._impl.exceeds_thresholds(
            future[_UTILIZATION_METRIC], "forecasted Redshift CPU Utilization"
        )

# Need to use maximum because we use this metric to estimate tail latency. The
# average CPU utilization includes the Redshift leader node, which is generally
# underutilized (and thus incorrectly biases the utilization value we use).
_UTILIZATION_METRIC = "CPUUtilization_Maximum"
