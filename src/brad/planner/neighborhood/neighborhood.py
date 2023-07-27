import asyncio
import logging
import pandas as pd
from typing import Dict, List
from pathlib import Path

from brad.config.engine import Engine
from brad.config.planner import PlannerConfig
from brad.planner.abstract import BlueprintPlanner
from brad.planner.enumeration.neighborhood import NeighborhoodBlueprintEnumerator
from brad.planner.neighborhood.filters import Filter
from brad.planner.neighborhood.filters.aurora_transactions import AuroraTransactions
from brad.planner.neighborhood.filters.no_data_loss import NoDataLoss
from brad.planner.neighborhood.filters.single_engine_execution import (
    SingleEngineExecution,
)
from brad.planner.neighborhood.filters.table_on_engine import TableOnEngine
from brad.planner.neighborhood.impl import NeighborhoodImpl
# from brad.planner.neighborhood.scaling_scorer import ALL_METRICS
from brad.planner.neighborhood.score import ScoringContext
from brad.planner.neighborhood.full_neighborhood import FullNeighborhoodSearchPlanner
from brad.planner.neighborhood.sampled_neighborhood import (
    SampledNeighborhoodSearchPlanner,
)
from brad.planner.strategy import PlanningStrategy
from brad.provisioning.directory import Directory
from brad.routing.rule_based import RuleBased
from brad.front_end.engine_connections import EngineConnections
from brad.utils.table_sizer import TableSizer

logger = logging.getLogger(__name__)

LOG_REPLAN_VAR = "BRAD_LOG_PLANNING"


class NeighborhoodSearchPlanner(BlueprintPlanner):
    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        # Workload independent semantic filters.
        self._workload_independent_filters: List[Filter] = [
            NoDataLoss(),
            TableOnEngine(),
        ]

        self._metrics_out = open(
            Path(self._config.planner_log_path) / "actual_metrics.csv",
            "a",
            encoding="UTF-8",
        )

        planner_config: PlannerConfig = kwargs["planner_config"]
        strategy = planner_config.strategy()

        if strategy == PlanningStrategy.FullNeighborhood:
            self._impl: NeighborhoodImpl = FullNeighborhoodSearchPlanner(planner_config)
        elif strategy == PlanningStrategy.SampledNeighborhood:
            self._impl = SampledNeighborhoodSearchPlanner(planner_config)
        else:
            assert False

    async def run_forever(self) -> None:
        try:
            while True:
                await asyncio.sleep(3)
                logger.debug("Planner is checking if a replan is needed...")
                if self._check_if_metrics_warrant_replanning():
                    await self.run_replan()
        finally:
            self._metrics_out.close()

    async def run_replan(self) -> None:
        # This will be long-running and will block the event loop. For our
        # current needs, this is fine since the planner is the main component in
        # the daemon process.
        logger.info("Running a replan.")
        self._log_current_metrics()
        next_workload = self._workload_provider.next_workload()
        workload_filters = [
            AuroraTransactions(next_workload),
            SingleEngineExecution(next_workload),
        ]

        # Establish connections to the underlying engines (needed for scoring
        # purposes).
        directory = Directory(self._config)
        await directory.refresh()
        engines = EngineConnections.connect_sync(
            self._config, directory, self._schema_name, autocommit=False
        )
        table_sizer = TableSizer(engines, self._config)

        try:
            # Load metrics.
            # metrics = self._monitor.read_k_most_recent(metric_ids=ALL_METRICS)
            # TODO: If needed, we need to transition this logic to the new metrics format.
            metrics = pd.DataFrame({})

            # Update the dataset size. We must use the current blueprint because it
            # contains information about where the tables are now.
            if self._current_workload.table_sizes_empty():
                self._current_workload.populate_table_sizes_using_blueprint(
                    self._current_blueprint, table_sizer
                )
                self._current_workload.set_dataset_size_from_table_sizes()

            if next_workload.table_sizes_empty():
                next_workload.populate_table_sizes_using_blueprint(
                    self._current_blueprint, table_sizer
                )
                next_workload.set_dataset_size_from_table_sizes()

            # Determine the amount of data accessed by the existing workload.
            data_accessed_mb = self._estimate_current_data_accessed(engines)

            # Used for all scoring.
            scoring_ctx = ScoringContext(
                self._current_blueprint,
                self._current_workload,
                next_workload,
                engines,
                metrics,
                data_accessed_mb,
            )

            self._impl.on_start_enumeration()

            for idx, bp in enumerate(
                NeighborhoodBlueprintEnumerator.enumerate(
                    self._current_blueprint,
                    self._planner_config.max_num_table_moves(),
                    self._planner_config.max_provisioning_multiplier(),
                )
            ):
                if idx % 10000 == 0:
                    logger.info("Processing %d", idx)

                # Workload-independent filters.
                # Drop this candidate if any are invalid.
                if any(
                    map(
                        # pylint: disable-next=cell-var-from-loop
                        lambda filt: not filt.is_valid(bp),
                        self._workload_independent_filters,
                    )
                ):
                    continue

                # Workload-specific filters.
                # Drop this candidate if any are invalid.
                # pylint: disable-next=cell-var-from-loop
                if any(map(lambda filt: not filt.is_valid(bp), workload_filters)):
                    continue

                self._impl.on_enumerated_blueprint(bp, scoring_ctx)

            selected_blueprint = self._impl.on_enumeration_complete(scoring_ctx)
            self._current_blueprint = selected_blueprint
            self._current_workload = next_workload
            await self._notify_new_blueprint(selected_blueprint)

        finally:
            engines.close_sync()

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return False

    def _estimate_current_data_accessed(
        self, engines: EngineConnections
    ) -> Dict[Engine, int]:
        current_router = RuleBased(blueprint=self._current_blueprint)

        total_accessed_mb: Dict[Engine, int] = {}
        total_accessed_mb[Engine.Aurora] = 0
        total_accessed_mb[Engine.Redshift] = 0
        total_accessed_mb[Engine.Athena] = 0

        # Compute the total amount of data accessed on each engine in the
        # current workload (used to weigh the workload assigned to each engine).
        for q in self._current_workload.analytical_queries():
            current_engine = current_router.engine_for_sync(q)
            q.populate_data_accessed_mb(
                current_engine, engines, self._current_blueprint
            )
            total_accessed_mb[current_engine] += q.data_accessed_mb(current_engine)

        return total_accessed_mb

    def _log_current_metrics(self) -> None:
        redshift_prov = self._current_blueprint.redshift_provisioning()
        aurora_prov = self._current_blueprint.aurora_provisioning()
        # TODO: If needed, we need to transition this logic to the new metrics format.
        # metrics = self._monitor.read_k_most_recent(metric_ids=ALL_METRICS)
        metrics = pd.DataFrame({})
        # Prepend provisioning information.
        metrics.insert(0, "redshift_instance_type", redshift_prov.instance_type())
        metrics.insert(1, "redshift_num_nodes", redshift_prov.num_nodes())
        metrics.insert(2, "aurora_instance_type", aurora_prov.instance_type())
        metrics.insert(3, "aurora_num_nodes", aurora_prov.num_nodes())
        string_csv = metrics.to_csv(index=False)
        self._metrics_out.write(string_csv)
        self._metrics_out.flush()
