import logging
from datetime import timedelta, datetime

from .trigger import Trigger

logger = logging.getLogger(__name__)


class ElapsedTimeTrigger(Trigger):
    def __init__(self, period: timedelta) -> None:
        super().__init__()
        self._period = period
        self._reset_trigger_next()

    def should_replan(self) -> bool:
        now = datetime.now()
        if now >= self._trigger_next:
            self._reset_trigger_next()
            logger.info(
                "Triggering replan due to expired timer (period: %s).", self._period
            )
            return True
        return False

    def _reset_trigger_next(self) -> None:
        self._trigger_next = datetime.now() + self._period
