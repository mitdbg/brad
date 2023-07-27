import pytz
import logging
from typing import Optional
from datetime import datetime

from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
from brad.utils.time_periods import period_start

logger = logging.getLogger(__name__)


class WorkloadProvider:
    """
    An abstract interface over a component that can provide the next workload
    (for blueprint planning purposes).
    """

    def next_workload(self) -> Workload:
        raise NotImplementedError


class FixedWorkloadProvider(WorkloadProvider):
    """
    Always returns the same workload. Used for debugging purposes.
    """

    def __init__(self, workload: Workload) -> None:
        self._workload = workload

    def next_workload(self) -> Workload:
        return self._workload


class LoggedWorkloadProvider(WorkloadProvider):
    """
    Returns the logged workload.
    """

    def __init__(self, config: ConfigFile, planner_config: PlannerConfig) -> None:
        self._config = config
        self._planner_config = planner_config
        self._workload: Optional[Workload] = None

    def next_workload(self) -> Workload:
        window_length = self._planner_config.planning_window()
        now = datetime.now().astimezone(pytz.utc)
        window_start = period_start(now, window_length)
        window_end = window_start + window_length

        builder = WorkloadBuilder()
        # TODO: This call should be async. But since we run it on the daemon,
        # it's probably fine.
        builder.add_queries_from_s3_logs(self._config, window_start, window_end)
        workload = builder.build()
        logger.debug(
            "LoggedWorkloadProvider loaded workload: %d unique A queries, %d T queries, period %s",
            len(workload.analytical_queries()),
            len(workload.transactional_queries()),
            workload.period(),
        )
        return workload
