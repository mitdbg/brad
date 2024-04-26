import logging
from datetime import timedelta
from typing import Optional

from brad.config.metrics import FrontEndMetric
from brad.daemon.monitor import Monitor
from brad.planner.triggers.trigger import Trigger

logger = logging.getLogger(__name__)


class TransactionLatencyCeiling(Trigger):
    def __init__(
        self,
        monitor: Monitor,
        latency_ceiling_s: float,
        sustained_epochs: int,
        epoch_length: timedelta,
        observe_bp_delay: timedelta,
        lookahead_epochs: Optional[int] = None,
    ) -> None:
        super().__init__(epoch_length, observe_bp_delay)
        self._monitor = monitor
        self._latency_ceiling_s = latency_ceiling_s
        self._sustained_epochs = sustained_epochs
        self._lookahead_epochs = lookahead_epochs

    def set_latency_ceiling(self, ceiling_s: float) -> None:
        self._latency_ceiling_s = ceiling_s

    async def should_replan(self) -> bool:
        if not self._passed_delays_since_cutoff():
            logger.debug(
                "Skippping transaction latency ceiling trigger because we have not passed the delay cutoff."
            )
            return False

        past = self._monitor.front_end_metrics().read_k_most_recent(
            k=self._sustained_epochs,
            metric_ids=[FrontEndMetric.TxnLatencySecondP90.value],
        )
        rel_past = past[past.index > self._cutoff]
        rel = rel_past[FrontEndMetric.TxnLatencySecondP90.value]
        if len(rel) >= self._sustained_epochs and (rel > self._latency_ceiling_s).all():
            p90_lat_s = rel.iloc[-1]
            logger.info(
                "Triggering replan because p90 transaction latency (%f) is above %f.",
                p90_lat_s,
                self._latency_ceiling_s,
            )
            return True

        if self._lookahead_epochs is None:
            return False

        if not self._passed_n_epochs_since_cutoff(self._sustained_epochs):
            return False

        future = self._monitor.front_end_metrics().read_k_upcoming(
            k=self._lookahead_epochs,
            metric_ids=[FrontEndMetric.TxnLatencySecondP90.value],
        )
        rel = future[FrontEndMetric.TxnLatencySecondP90.value]
        rel = rel[-self._sustained_epochs :]
        if len(rel) < self._sustained_epochs:
            return False

        if (rel > self._latency_ceiling_s).all():
            p90_lat_s = rel.iloc[-1]
            logger.info(
                "Triggering replan because p90 transaction latency (%f) is forecasted to be above %f.",
                p90_lat_s,
                self._latency_ceiling_s,
            )
            return True

        return False
