import logging
from datetime import timedelta, datetime
from typing import Optional

from brad.blueprint import Blueprint
from brad.blueprint.diff.blueprint import BlueprintDiff
from brad.config.planner import PlannerConfig
from brad.planner.scoring.score import Score
from brad.utils.time_periods import universal_now

from .trigger import Trigger

logger = logging.getLogger(__name__)


class RecentChange(Trigger):
    """
    This triggers a replan if there was a recent provisioning change.
    """

    def __init__(
        self,
        planner_config: PlannerConfig,
        epoch_length: timedelta,
        delay_epochs: int,
        observe_bp_delay: timedelta,
    ) -> None:
        super().__init__(epoch_length, observe_bp_delay)
        self._planner_config = planner_config
        self._is_first_change = True
        self._last_provisioning_change: Optional[datetime] = None
        self._delay_epochs = delay_epochs

    async def should_replan(self) -> bool:
        if self._last_provisioning_change is None:
            return False

        now = universal_now()
        if not self._passed_delays_since_cutoff(now):
            logger.debug(
                "Skippping recent change trigger because we have not passed the delay cutoff."
            )
            return False

        delay_window = self._delay_epochs * self._epoch_length

        if now > self._last_provisioning_change + delay_window + self._total_delay():
            self._last_provisioning_change = None
            logger.info("Triggering replan because of a recent provisioning change.")
            return True
        else:
            return False

    def on_replan(self, trigger: Optional["Trigger"]) -> None:
        logger.debug(
            "Clearing RecentChange trigger replan state due to %s firing",
            trigger.name() if trigger is not None else "manual",
        )
        self._last_provisioning_change = None

    def update_blueprint(self, blueprint: Blueprint, score: Optional[Score]) -> None:
        if self._is_first_change or self._current_blueprint is None:
            self._is_first_change = False
            super().update_blueprint(blueprint, score)
            return

        prev_blueprint = self._current_blueprint
        super().update_blueprint(blueprint, score)

        diff = BlueprintDiff.of(prev_blueprint, blueprint)
        if diff is None:
            self._last_provisioning_change = None
            return

        aurora_diff = diff.aurora_diff()
        redshift_diff = diff.redshift_diff()
        if aurora_diff is not None or redshift_diff is not None:
            self._last_provisioning_change = universal_now()
            logger.info(
                "RecentChangeTrigger: Will trigger one planning window after %s",
                self._last_provisioning_change.strftime("%Y-%m-%d_%H-%M-%S"),
            )
