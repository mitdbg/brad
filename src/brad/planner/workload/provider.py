import pytz
import logging
from typing import Optional
from datetime import datetime

from brad.blueprint_manager import BlueprintManager
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
from brad.utils.time_periods import period_start
from brad.utils.table_sizer import TableSizer
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class WorkloadProvider:
    """
    An abstract interface over a component that can provide the next workload
    (for blueprint planning purposes).
    """

    def next_workload(self, window_multiplier: int = 1) -> Workload:
        raise NotImplementedError


class FixedWorkloadProvider(WorkloadProvider):
    """
    Always returns the same workload. Used for debugging purposes.
    """

    def __init__(self, workload: Workload) -> None:
        self._workload = workload

    def next_workload(self, window_multiplier: int = 1) -> Workload:
        return self._workload


class LoggedWorkloadProvider(WorkloadProvider):
    """
    Returns the logged workload.
    """

    def __init__(
        self,
        config: ConfigFile,
        planner_config: PlannerConfig,
        blueprint_mgr: BlueprintManager,
        schema_name: str,
    ) -> None:
        self._config = config
        self._planner_config = planner_config
        self._workload: Optional[Workload] = None
        self._blueprint_mgr = blueprint_mgr
        self._schema_name = schema_name

    def next_workload(self, window_multiplier: int = 1) -> Workload:
        window_length = self._planner_config.planning_window() * window_multiplier
        now = datetime.now().astimezone(pytz.utc)
        window_start = period_start(now, window_length)
        window_end = window_start + window_length
        logger.debug("Retrieving workload in range %s -- %s", window_start, window_end)

        ec = EngineConnections.connect_sync(
            self._config, self._blueprint_mgr.get_directory(), self._schema_name
        )
        table_sizer = TableSizer(ec, self._config)

        builder = WorkloadBuilder()
        # TODO: This call should be async. But since we run it on the daemon,
        # it's probably fine.
        builder.add_queries_from_s3_logs(self._config, window_start, window_end)
        builder.table_sizes_from_engines(
            self._blueprint_mgr.get_blueprint(), table_sizer
        )
        workload = builder.build()
        logger.debug(
            "LoggedWorkloadProvider loaded workload: %d unique A queries, %d T queries, period %s",
            len(workload.analytical_queries()),
            len(workload.transactional_queries()),
            workload.period(),
        )
        ec.close_sync()
        return workload
