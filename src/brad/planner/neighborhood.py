import asyncio
import logging
from typing import List

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner import BlueprintPlanner
from brad.planner.enumeration.neighborhood import NeighborhoodBlueprintEnumerator
from brad.planner.filters import Filter
from brad.planner.filters.aurora_transactions import AuroraTransactions
from brad.planner.filters.no_data_loss import NoDataLoss
from brad.planner.filters.single_engine_execution import SingleEngineExecution
from brad.planner.filters.table_on_engine import TableOnEngine
from brad.planner.scoring.scaling_scorer import ScalingScorer
from brad.planner.workload import Workload
from brad.server.engine_connections import EngineConnections
from brad.utils.table_sizer import TableSizer

logger = logging.getLogger(__name__)


class NeighborhoodSearchPlanner(BlueprintPlanner):
    def __init__(
        self,
        current_blueprint: Blueprint,
        current_workload: Workload,
        planner_config: PlannerConfig,
        monitor: Monitor,
        config: ConfigFile,
        schema_name: str,
    ) -> None:
        super().__init__()
        self._current_blueprint = current_blueprint
        self._current_workload = current_workload
        # The intention is to decouple the planner and monitor down the line
        # when it is clear how we want to process the metrics provided by the
        # monitor.
        self._monitor = monitor

        # Workload independent semantic filters.
        self._workload_independent_filters: List[Filter] = [
            NoDataLoss(),
            TableOnEngine(),
        ]
        self._planner_config = planner_config
        self._config = config
        self._schema_name = schema_name
        self._scorer = ScalingScorer(self._monitor, self._planner_config)

    async def run_forever(self) -> None:
        while True:
            await asyncio.sleep(3)
            logger.debug("Planner is checking if a replan is needed...")
            if self._check_if_metrics_warrant_replanning():
                await self._replan()

    async def _replan(self) -> None:
        # This will be long-running and will block the event loop. For our
        # current needs, this is fine since the planner is the main component in
        # the daemon process.
        logger.info("Running a replan.")
        next_workload = self._expected_workload()
        workload_filters = [
            AuroraTransactions(next_workload),
            SingleEngineExecution(next_workload),
        ]

        # No need to keep around all candidates if we are selecting the best
        # blueprint. But for debugging purposes it is useful to see what
        # blueprints are being considered.
        candidate_set = []

        # Establish connections to the underlying engines (needed for scoring
        # purposes). We use synchronous connections since there appears to be a
        # bug in aioodbc that causes an indefinite await on a query result.
        engines = EngineConnections.connect_sync(
            self._config, self._schema_name, autocommit=False
        )
        table_sizer = TableSizer(engines, self._config)

        try:
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

            for bp in NeighborhoodBlueprintEnumerator.enumerate(
                self._current_blueprint,
                self._planner_config.max_num_table_moves(),
                self._planner_config.max_provisioning_multiplier(),
            ):
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

                # Score the blueprint.
                score = self._scorer.score(
                    self._current_blueprint,
                    bp,
                    self._current_workload,
                    next_workload,
                    engines,
                )

                # Store the blueprint (for debugging purposes).
                candidate_set.append((score, bp.to_blueprint()))

            # Sort by score - lower is better.
            candidate_set.sort(key=lambda parts: parts[0].single_value())

            # Log the top 50 candidate plans.
            for score, candidate in candidate_set[:50]:
                logger.debug("%s", score)
                logger.debug("%s", candidate)
                logger.debug("----------")

            if len(candidate_set) == 0:
                logger.error("Planner did not find any valid candidate blueprints.")
                logger.error("Next workload: %s", next_workload)
                raise RuntimeError("No valid candidates!")

            best_score, best_blueprint = candidate_set[1]
            logger.info("Selecting a new blueprint with score %s", best_score)
            logger.info("%s", best_blueprint)
            self._current_blueprint = best_blueprint
            self._current_workload = next_workload

            # Emit the next blueprint.
            await self._notify_new_blueprint(best_blueprint)

        finally:
            engines.close_sync()

    def _check_if_metrics_warrant_replanning(self) -> bool:
        # See if the metrics indicate that we should trigger the planning
        # process.
        return True

    def _expected_workload(self) -> Workload:
        return self._current_workload
