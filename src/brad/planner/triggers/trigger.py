from typing import Optional
from brad.blueprint import Blueprint


class Trigger:
    def __init__(self) -> None:
        self._current_blueprint: Optional[Blueprint] = None

    def should_replan(self) -> bool:
        """
        Returns true if the blueprint planner should run again. This method is
        meant to be called periodically.
        """
        raise NotImplementedError

    def update_blueprint(self, blueprint: Blueprint) -> None:
        self._current_blueprint = blueprint

    def name(self) -> str:
        """
        The name of the trigger.
        """
        return self.__class__.__name__
