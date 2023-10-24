import logging
import math
import pytz
import pandas as pd
from datetime import datetime, timedelta
from typing import List

from .trigger import Trigger
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.router_provider import RouterProvider
from brad.planner.scoring.data_access.provider import DataAccessProvider
from brad.planner.workload.builder import WorkloadBuilder
from brad.planner.workload.query import Query
from brad.planner.scoring.provisioning import (
    compute_aurora_scan_cost,
    compute_athena_scan_cost,
    compute_aurora_accessed_pages,
    compute_athena_scanned_bytes,
)

logger = logging.getLogger(__name__)


class VariableCosts(Trigger):
    def __init__(
        self,
        config: ConfigFile,
        planner_config: PlannerConfig,
        monitor: Monitor,
        data_access_provider: DataAccessProvider,
        router_provider: RouterProvider,
        threshold_frac: float,
    ) -> None:
        """
        This will trigger a replan if the current variable costs (currently,
        just scans) exceeds the previously estimated scan cost by more than
        `threshold_frac` in either direction.

        For example, if `threshold_frac` is 0.2, then replanning is triggered if
        the estimated cost is +/- 20% of the previously estimated cost.
        """
        super().__init__()
        self._config = config
        self._planner_config = planner_config
        self._monitor = monitor
        self._data_access_provider = data_access_provider
        self._router_provider = router_provider
        self._change_ratio = 1.0 + threshold_frac

    async def should_replan(self) -> bool:
        if self._current_blueprint is None or self._current_score is None:
            # We have no reference point for what the expected variable cost
            # should be.
            logger.debug(
                "VariableCosts trigger not running because there is no reference point."
            )
            return False

        current_hourly_cost = await self._estimate_current_scan_hourly_cost()
        if current_hourly_cost <= 1e-5:
            # Treated as 0.
            logger.debug(
                "Current hourly scan cost computed to be 0. VariableCosts not triggering."
            )
            return False

        estimated_hourly_cost = self._current_score.workload_scan_cost
        ratio = max(
            current_hourly_cost / estimated_hourly_cost,
            estimated_hourly_cost / current_hourly_cost,
        )

        if ratio > self._change_ratio:
            logger.info(
                "Triggering replanning due to variable costs changing. Previously estimated: %.4f. Current estimated: %.4f. Change ratio: %.4f",
                estimated_hourly_cost,
                current_hourly_cost,
                self._change_ratio,
            )
            return True

        return False

    async def _estimate_current_scan_hourly_cost(self) -> float:
        if self._current_blueprint is None:
            return 0.0

        # Extract the queries seen in the last window.
        window_end = datetime.now()
        window_end = window_end.astimezone(pytz.utc)
        window_start = (
            window_end
            - self._planner_config.planning_window()
            - self._config.epoch_length
        )
        logger.debug("Variable costs range: %s -- %s", window_start, window_end)
        workload = (
            WorkloadBuilder()
            .add_queries_from_s3_logs(self._config, window_start, window_end)
            .build(
                rescale_to_period=timedelta(hours=1),
                reinterpret_second_as=self._planner_config.reinterpret_second_as(),
            )
        )
        if len(workload.analytical_queries()) == 0:
            return 0.0
        self._data_access_provider.apply_access_statistics(workload)

        # Compute the scan cost of this last window of queries.
        aurora_query_indices: List[int] = []
        aurora_queries: List[Query] = []
        athena_query_indices: List[int] = []
        athena_queries: List[Query] = []
        router = await self._router_provider.get_router(
            self._current_blueprint.table_locations_bitmap()
        )

        for idx, q in enumerate(workload.analytical_queries()):
            engine = await router.engine_for(q)
            if engine == Engine.Aurora:
                aurora_query_indices.append(idx)
                aurora_queries.append(q)
            elif engine == Engine.Athena:
                athena_query_indices.append(idx)
                athena_queries.append(q)

        aurora_accessed_pages = compute_aurora_accessed_pages(
            aurora_queries,
            workload.get_predicted_aurora_pages_accessed_batch(aurora_query_indices),
        )
        athena_scanned_bytes = compute_athena_scanned_bytes(
            athena_queries,
            workload.get_predicted_athena_bytes_accessed_batch(athena_query_indices),
            self._planner_config,
        )

        # We use the hit rate to estimate Aurora scan costs.
        lookback_epochs = math.ceil(
            self._planner_config.planning_window() / self._config.epoch_length
        )
        # TODO: If there are read replicas, we should use the hit rate from them instead.
        aurora_reader_metrics = self._monitor.aurora_reader_metrics()
        if len(aurora_reader_metrics) > 0:
            reader_hit_rates = []
            for reader_metrics in aurora_reader_metrics:
                reader_hit_rates.append(
                    reader_metrics.read_k_most_recent(
                        k=lookback_epochs, metric_ids=[_HIT_RATE_METRIC]
                    )
                )
            all_metrics = pd.concat(reader_hit_rates)
            hit_rate_avg = all_metrics[_HIT_RATE_METRIC].mean() / 100.0
        else:
            aurora_writer_metrics = self._monitor.aurora_writer_metrics()
            metrics = aurora_writer_metrics.read_k_most_recent(
                k=lookback_epochs, metric_ids=[_HIT_RATE_METRIC]
            )
            hit_rate_avg = metrics[_HIT_RATE_METRIC].mean() / 100.0

        aurora_scan_cost = compute_aurora_scan_cost(
            aurora_accessed_pages, hit_rate_avg, self._planner_config
        )
        athena_scan_cost = compute_athena_scan_cost(
            athena_scanned_bytes, self._planner_config
        )

        return aurora_scan_cost + athena_scan_cost


_HIT_RATE_METRIC = "BufferCacheHitRatio_Average"
