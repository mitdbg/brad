import asyncio
import logging
from typing import Coroutine, Callable, List, Optional, Iterable, Tuple

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.config.system_event import SystemEvent
from brad.daemon.system_event_logger import SystemEventLogger
from brad.planner.providers import BlueprintProviders
from brad.planner.scoring.score import Score
from brad.planner.triggers.trigger import Trigger

logger = logging.getLogger(__name__)

NewBlueprintCallback = Callable[
    [Blueprint, Score, Optional[Trigger]], Coroutine[None, None, None]
]


class BlueprintPlanner:
    """
    `BlueprintPlanner`s should not run in the same process as the BRAD server.
    The optimization process will be long running and Python's GIL prevents
    multiple Python threads from executing in parallel.
    """

    def __init__(
        self,
        config: ConfigFile,
        planner_config: PlannerConfig,
        schema_name: str,
        current_blueprint: Blueprint,
        current_blueprint_score: Optional[Score],
        providers: BlueprintProviders,
        system_event_logger: Optional[SystemEventLogger],
    ) -> None:
        self._config = config
        self._planner_config = planner_config
        self._schema_name = schema_name
        self._current_blueprint = current_blueprint
        self._current_blueprint_score = current_blueprint_score
        self._last_suggested_blueprint: Optional[Blueprint] = None
        self._last_suggested_blueprint_score: Optional[Score] = None

        self._providers = providers
        self._system_event_logger = system_event_logger

        self._callbacks: List[NewBlueprintCallback] = []
        self._replan_in_progress = False
        self._disable_triggers = False

        self._triggers = self._providers.trigger_provider.get_triggers()
        for t in self._triggers:
            t.update_blueprint(self._current_blueprint, self._current_blueprint_score)

    async def run_forever(self) -> None:
        """
        Called to start the planner. The planner is meant to run until its task
        is cancelled.
        """
        if not self._planner_config.triggers_enabled():
            logger.info("Blueprint planner triggers are disabled.")
            # No need to do any further checks. Only manually triggered replans
            # are possible.
            return

        triggers = self.get_triggers()
        logger.info("Planner triggers:")
        for t in triggers:
            logger.info("- Trigger: %s", t.name())

        trigger_configs = self._planner_config.trigger_configs()
        check_offset = trigger_configs["check_period_offset_s"]
        check_period = trigger_configs["check_period_s"]

        await asyncio.sleep(check_offset + check_period)
        while True:
            logger.debug("Planner is checking if a replan is needed...")
            if not self._replan_in_progress and not self._disable_triggers:
                for t in self.get_triggers():
                    if await t.should_replan():
                        logger.info("Starting a triggered replan...")
                        if self._system_event_logger is not None:
                            self._system_event_logger.log(
                                SystemEvent.TriggeredReplan,
                                "trigger={}".format(t.name()),
                            )
                        await self.run_replan(trigger=t)
                        break
            else:
                logger.debug(
                    "A replan is already in progress or triggers are temporarily disabled. Skipping the trigger check."
                )
            await asyncio.sleep(check_period)

    async def run_replan(
        self, trigger: Optional[Trigger], window_multiplier: int = 1
    ) -> None:
        """
        Initiates a replan. Call this directly to "force" a replan.

        Use `window_multiplier` to expand the window used for planning.
        """
        try:
            self._replan_in_progress = True
            for t in self.get_triggers():
                t.on_replan(trigger)

            result = await self._run_replan_impl(window_multiplier)
            if result is None:
                return
            blueprint, score = result
            self._last_suggested_blueprint = blueprint
            self._last_suggested_blueprint_score = score

            await self._notify_new_blueprint(blueprint, score, trigger)

        finally:
            self._replan_in_progress = False

    async def _run_replan_impl(
        self, window_multiplier: int = 1
    ) -> Optional[Tuple[Blueprint, Score]]:
        """
        Implementers should override this method to define the blueprint
        optimization algorithm.
        """
        raise NotImplementedError

    def get_triggers(self) -> Iterable[Trigger]:
        """
        The triggers used to trigger blueprint replanning.
        """
        return self._triggers

    def set_disable_triggers(self, disable: bool) -> None:
        """
        Used to pause automatic replan triggers. This is used during a blueprint
        transition.
        """
        logger.info("Setting disable planner triggers: %s", str(disable))
        self._disable_triggers = disable

    def update_blueprint(self, blueprint: Blueprint, score: Optional[Score]) -> None:
        """
        Use this method to inform the planner of a new blueprint being
        transitioned to successfully.

        We need this method because the blueprints emitted by the planner are
        not immediately transitioned to (it takes time to transition
        blueprints).
        """
        self._current_blueprint = blueprint
        self._current_blueprint_score = score

        for t in self.get_triggers():
            t.update_blueprint(blueprint, score)

    # NOTE: In the future we will implement an abstraction that will allow for a
    # generic planner to subscribe to a stream of events, used to detect when to
    # trigger re-planning.

    def register_new_blueprint_callback(self, callback: NewBlueprintCallback) -> None:
        """
        Register a function to be called when this planner selects a new
        `Blueprint`.
        """
        self._callbacks.append(callback)

    async def _notify_new_blueprint(
        self, blueprint: Blueprint, score: Score, trigger: Optional[Trigger]
    ) -> None:
        """
        Concrete planners should call this method to notify subscribers about
        the next blueprint.
        """
        tasks = []
        for callback in self._callbacks:
            tasks.append(asyncio.create_task(callback(blueprint, score, trigger)))
        await asyncio.gather(*tasks)

    def replan_in_progress(self) -> bool:
        return self._replan_in_progress
