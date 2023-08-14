import asyncio
import logging
from typing import Coroutine, Callable, List, Optional, Iterable

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.compare.function import BlueprintComparator
from brad.planner.estimator import EstimatorProvider
from brad.planner.metrics import MetricsProvider
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.scoring.score import Score
from brad.planner.triggers.trigger import Trigger
from brad.planner.workload.provider import WorkloadProvider

logger = logging.getLogger(__name__)

NewBlueprintCallback = Callable[[Blueprint, Score], Coroutine[None, None, None]]


class BlueprintPlanner:
    """
    `BlueprintPlanner`s should not run in the same process as the BRAD server.
    The optimization process will be long running and Python's GIL prevents
    multiple Python threads from executing in parallel.
    """

    def __init__(
        self,
        planner_config: PlannerConfig,
        current_blueprint: Blueprint,
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
        workload_provider: WorkloadProvider,
        analytics_latency_scorer: AnalyticsLatencyScorer,
        comparator: BlueprintComparator,
        metrics_provider: MetricsProvider,
        data_access_provider: DataAccessProvider,
        estimator_provider: EstimatorProvider,
    ) -> None:
        self._planner_config = planner_config
        self._current_blueprint = current_blueprint
        # TODO: Ideally we persist and load the previous score as well.
        self._current_blueprint_score: Optional[Score] = None
        self._last_suggested_blueprint: Optional[Blueprint] = None
        self._last_suggested_blueprint_score: Optional[Score] = None
        self._monitor = monitor
        self._config = config
        self._schema_name = schema_name

        self._workload_provider = workload_provider
        self._analytics_latency_scorer = analytics_latency_scorer
        self._comparator = comparator
        self._metrics_provider = metrics_provider
        self._data_access_provider = data_access_provider
        self._estimator_provider = estimator_provider

        self._callbacks: List[NewBlueprintCallback] = []

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

        trigger_configs = self._planner_config.trigger_configs()
        check_offset = trigger_configs["check_period_offset_s"]
        check_period = trigger_configs["check_period_s"]

        await asyncio.sleep(check_offset + check_period)
        while True:
            logger.debug("Planner is checking if a replan is needed...")
            for t in self.get_triggers():
                if await t.should_replan():
                    logger.info("Starting a triggered replan...")
                    await self.run_replan()
                    break
            await asyncio.sleep(check_period)

    async def run_replan(self, window_multiplier: int = 1) -> None:
        """
        Triggers a "forced" replan. Used for debugging.

        Use `window_multiplier` to expand the window used for planning.
        """
        raise NotImplementedError

    def get_triggers(self) -> Iterable[Trigger]:
        """
        Implementers should return the triggers used to trigger blueprint
        replanning.
        """
        raise NotImplementedError

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

    async def _notify_new_blueprint(self, blueprint: Blueprint, score: Score) -> None:
        """
        Concrete planners should call this method to notify subscribers about
        the next blueprint.
        """
        tasks = []
        for callback in self._callbacks:
            tasks.append(asyncio.create_task(callback(blueprint, score)))
        await asyncio.gather(*tasks)
