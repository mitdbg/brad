import logging
from typing import Optional

from brad.config.metrics import FrontEndMetric
from brad.daemon.monitor import Monitor
from brad.planner.triggers.trigger import Trigger

logger = logging.getLogger(__name__)


class QueryLatencyCeiling(Trigger):
    def __init__(
        self,
        monitor: Monitor,
        latency_ceiling_s: float,
        sustained_epochs: int,
        lookahead_epochs: Optional[int] = None,
    ) -> None:
        super().__init__()
        self._monitor = monitor
        self._latency_ceiling_s = latency_ceiling_s
        self._sustained_epochs = sustained_epochs
        self._lookahead_epochs = lookahead_epochs

    async def should_replan(self) -> bool:
        past = self._monitor.front_end_metrics().read_k_most_recent(
            k=self._sustained_epochs,
            metric_ids=[FrontEndMetric.QueryLatencyMaxSecond.value],
        )
        rel = past[FrontEndMetric.QueryLatencyMaxSecond.value]
        if len(rel) >= self._sustained_epochs and (rel > self._latency_ceiling_s).all():
            max_lat_s = rel.iloc[-1]
            logger.info(
                "Triggering replan because maximum query latency (%f) is above %f.",
                max_lat_s,
                self._latency_ceiling_s,
            )
            return True

        if self._lookahead_epochs is None:
            return False

        future = self._monitor.front_end_metrics().read_k_upcoming(
            k=self._lookahead_epochs,
            metric_ids=[FrontEndMetric.QueryLatencyMaxSecond.value],
        )
        rel = future[FrontEndMetric.QueryLatencyMaxSecond.value]
        rel = rel[-self._sustained_epochs :]
        if len(rel) < self._sustained_epochs:
            return False

        if (rel > self._latency_ceiling_s).all():
            max_lat_s = rel.iloc[-1]
            logger.info(
                "Triggering replan because maximum query latency (%f) is forecasted to be above %f.",
                max_lat_s,
                self._latency_ceiling_s,
            )
            return True

        return False