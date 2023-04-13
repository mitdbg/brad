import asyncio
import logging

from brad.blueprint import Blueprint
from brad.daemon.monitor import Monitor
from brad.planner import BlueprintPlanner

logger = logging.getLogger(__name__)


class NeighborhoodSearchPlanner(BlueprintPlanner):
    def __init__(self, current_blueprint: Blueprint, monitor: Monitor) -> None:
        super().__init__()
        self._current_blueprint = current_blueprint
        # The intention is to decouple the planner and monitor down the line
        # when it is clear how we want to process the metrics provided by the
        # monitor.
        self._monitor = monitor

    async def run_forever(self) -> None:
        while True:
            logger.debug("Planner is running...")
            await asyncio.sleep(3)
            if self._check_if_metrics_warrant_replanning():
                # Trigger the replanning
                pass

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return False
