import logging
from datetime import timedelta
from typing import Optional

from brad.blueprint import Blueprint
from brad.planner.scoring.score import Score
from brad.utils.time_periods import universal_now

from .trigger import Trigger

logger = logging.getLogger(__name__)


class ElapsedTimeTrigger(Trigger):
    def __init__(self, period: timedelta, epoch_length: timedelta) -> None:
        super().__init__(epoch_length)
        self._period = period
        self._reset_trigger_next()

    async def should_replan(self) -> bool:
        now = universal_now()
        if now >= self._trigger_next:
            self._reset_trigger_next()
            logger.info(
                "Triggering replan due to expired timer (period: %s).", self._period
            )
            return True
        return False

    def update_blueprint(self, blueprint: Blueprint, score: Optional[Score]) -> None:
        super().update_blueprint(blueprint, score)
        self._reset_trigger_next()

    def _reset_trigger_next(self) -> None:
        self._trigger_next = self._cutoff + self._period
