import asyncio
from typing import Coroutine, Callable, List

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.compare.function import BlueprintComparator
from brad.planner.metrics import MetricsProvider
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.scoring.performance.analytics_latency import AnalyticsLatencyScorer
from brad.planner.workload import Workload
from brad.planner.workload.provider import WorkloadProvider

NewBlueprintCallback = Callable[[Blueprint], Coroutine[None, None, None]]


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
        current_workload: Workload,
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
        workload_provider: WorkloadProvider,
        analytics_latency_scorer: AnalyticsLatencyScorer,
        comparator: BlueprintComparator,
        metrics_provider: MetricsProvider,
        data_access_provider: DataAccessProvider,
    ) -> None:
        self._planner_config = planner_config
        self._current_blueprint = current_blueprint
        self._current_workload = current_workload
        self._monitor = monitor
        self._config = config
        self._schema_name = schema_name

        self._workload_provider = workload_provider
        self._analytics_latency_scorer = analytics_latency_scorer
        self._comparator = comparator
        self._metrics_provider = metrics_provider
        self._data_access_provider = data_access_provider

        self._callbacks: List[NewBlueprintCallback] = []

    async def run_forever(self) -> None:
        """
        Called to start the planner. The planner is meant to run until its task
        is cancelled.
        """
        raise NotImplementedError

    async def run_replan(self) -> None:
        """
        Triggers a "forced" replan. Used for debugging.
        """
        raise NotImplementedError

    # NOTE: In the future we will implement an abstraction that will allow for a
    # generic planner to subscribe to a stream of events, used to detect when to
    # trigger re-planning.

    def register_new_blueprint_callback(self, callback: NewBlueprintCallback) -> None:
        """
        Register a function to be called when this planner selects a new
        `Blueprint`.
        """
        self._callbacks.append(callback)

    async def _notify_new_blueprint(self, blueprint: Blueprint) -> None:
        """
        Concrete planners should call this method to notify subscribers about
        the next blueprint.
        """
        tasks = []
        for callback in self._callbacks:
            tasks.append(asyncio.create_task(callback(blueprint)))
        await asyncio.gather(*tasks)
