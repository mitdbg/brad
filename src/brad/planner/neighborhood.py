import asyncio
import logging

from brad.blueprint import Blueprint
from brad.planner import BlueprintPlanner

logger = logging.getLogger(__name__)


class NeighborhoodSearchPlanner(BlueprintPlanner):
    async def run_forever(self) -> None:
        while True:
            logger.debug("Planner is running...")
            await asyncio.sleep(3)
            await self._notify_new_blueprint(Blueprint("test", [], None, None, None))
