import logging
from typing import Optional, Tuple
from datetime import datetime, timedelta

from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
from brad.utils.table_sizer import TableSizer
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class WorkloadProvider:
    """
    An abstract interface over a component that provides the current and next
    workload (for blueprint planning purposes).
    """

    async def get_workloads(
        self,
        window_end: datetime,
        window_multiplier: int = 1,
        desired_period: Optional[timedelta] = None,
    ) -> Tuple[Workload, Workload]:
        """
        Retrieves the current and next workload.

        Use `window_end` to specify the endpoint of the workload "look behind"
        window. We use this to (i) prevent trying to read query logs that have not
        yet been uploaded, and (ii) to only read logs that correspond with the
        metrics being used.

        Use `window_multiplier` to expand the window used for extracting the
        workload from the query logs.
        """
        # TODO: Might be a good idea to differentiate the "current" workload
        # provider from the "next" workload provider.
        raise NotImplementedError


class FixedWorkloadProvider(WorkloadProvider):
    """
    Always returns the same workload. Used for debugging purposes.
    """

    def __init__(self, workload: Workload) -> None:
        self._workload_curr = workload
        self._workload_next = workload.clone()

    async def get_workloads(
        self,
        window_end: datetime,
        window_multiplier: int = 1,
        desired_period: Optional[timedelta] = None,
    ) -> Tuple[Workload, Workload]:
        return self._workload_curr, self._workload_next


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
        system_startup_time: datetime,
    ) -> None:
        self._config = config
        self._planner_config = planner_config
        self._workload: Optional[Workload] = None
        self._blueprint_mgr = blueprint_mgr
        self._schema_name = schema_name
        self._system_startup_time = system_startup_time

    async def get_workloads(
        self,
        window_end: datetime,
        window_multiplier: int = 1,
        desired_period: Optional[timedelta] = None,
    ) -> Tuple[Workload, Workload]:
        window_length = self._planner_config.planning_window() * window_multiplier
        window_start = window_end - window_length
        if window_start < self._system_startup_time:
            logger.info(
                "Adjusting lookback window to start at system startup: %s",
                self._system_startup_time.strftime("%Y-%m-%d %H:%M:%S,%f"),
            )
            window_start = self._system_startup_time
        logger.debug(
            "Retrieving workload in range %s -- %s. Length: %s",
            window_start,
            window_end,
            window_length,
        )

        bp = self._blueprint_mgr.get_blueprint()
        engines = {Engine.Athena}

        if bp.aurora_provisioning().num_nodes() > 0:
            engines.add(Engine.Aurora)
        if bp.redshift_provisioning().num_nodes() > 0:
            engines.add(Engine.Redshift)

        ec = EngineConnections.connect_sync(
            self._config,
            self._blueprint_mgr.get_directory(),
            self._schema_name,
            specific_engines=engines,
        )
        try:
            table_sizer = TableSizer(ec, self._config)

            builder = WorkloadBuilder()
            # TODO: These calls should be async. But since we run them on the
            # daemon, it's probably fine.
            builder.add_queries_from_s3_logs(self._config, window_start, window_end)
            await builder.table_sizes_from_engines(
                self._blueprint_mgr.get_blueprint(), table_sizer
            )
            workload = builder.build(
                rescale_to_period=desired_period,
                reinterpret_second_as=self._planner_config.reinterpret_second_as(),
            )
            logger.debug(
                "LoggedWorkloadProvider loaded workload: %d unique A queries, %d T queries, period %s",
                len(workload.analytical_queries()),
                len(workload.transactional_queries()),
                workload.period(),
            )
            return workload, workload.clone()
        finally:
            ec.close_sync()
