from typing import Optional
from datetime import timedelta, datetime
from brad.blueprint import Blueprint
from brad.daemon.aurora_metrics import AuroraMetrics
from brad.daemon.redshift_metrics import RedshiftMetrics
from brad.planner.scoring.score import Score
from brad.utils.time_periods import universal_now


class Trigger:
    def __init__(self, epoch_length: timedelta, observe_bp_delay: timedelta) -> None:
        self._current_blueprint: Optional[Blueprint] = None
        self._current_score: Optional[Score] = None
        self._epoch_length = epoch_length
        # Metrics sources can be delayed. We may not want to fire some triggers
        # before the metrics are available in the planning window.
        self._metrics_delay = max(
            AuroraMetrics.METRICS_DELAY, RedshiftMetrics.METRICS_DELAY
        )
        self._observe_bp_delay = observe_bp_delay
        self._reset_cutoff()

    async def should_replan(self) -> bool:
        """
        Returns true if the blueprint planner should run again. This method is
        meant to be called periodically.
        """
        raise NotImplementedError

    def update_blueprint(self, blueprint: Blueprint, score: Optional[Score]) -> None:
        self._current_blueprint = blueprint
        self._current_score = score
        # Used by triggers that want to avoid firing immediately after a
        # blueprint transition.
        self._reset_cutoff()

    def name(self) -> str:
        """
        The name of the trigger.
        """
        return self.__class__.__name__

    def on_replan(self, trigger: Optional["Trigger"]) -> None:
        """
        Called when a replan occurs (for stateful triggers). The trigger that
        fired the replan will be passed in; it will be None if the replan was
        triggered manually.
        """

    def _reset_cutoff(self) -> None:
        # This is when the last blueprint took affect.
        self._cutoff = universal_now()

    def _passed_n_epochs_since_cutoff(self, n: int) -> bool:
        elapsed = universal_now() - self._cutoff
        return elapsed >= n * self._epoch_length

    def _passed_delays_since_cutoff(
        self, reference_timestamp: Optional[datetime] = None
    ) -> bool:
        if reference_timestamp is None:
            now_ts = universal_now()
        else:
            now_ts = reference_timestamp
        return now_ts > self._cutoff + self._total_delay()

    def _total_delay(self) -> timedelta:
        return self._metrics_delay + self._observe_bp_delay
