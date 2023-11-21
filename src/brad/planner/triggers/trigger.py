from typing import Optional
from datetime import timedelta
from brad.blueprint import Blueprint
from brad.planner.scoring.score import Score
from brad.utils.time_periods import universal_now


class Trigger:
    def __init__(self, epoch_length: timedelta) -> None:
        self._current_blueprint: Optional[Blueprint] = None
        self._current_score: Optional[Score] = None
        self._epoch_length = epoch_length
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
        self._cutoff = universal_now()

    def _passed_n_epochs_since_cutoff(self, n: int) -> bool:
        elapsed = universal_now() - self._cutoff
        return elapsed >= n * self._epoch_length
