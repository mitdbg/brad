import asyncio
import csv
import logging
import heapq
from typing import Dict, List
from pathlib import Path

from brad.blueprint import Blueprint
from brad.config.engine import Engine
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
from brad.planner.scoring.scaling_scorer import ScalingScorer, ALL_METRICS
from brad.planner.scoring.score import Score, ScoringContext
from brad.planner.workload import Workload
from brad.routing.rule_based import RuleBased
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
        self._scorer = ScalingScorer(self._planner_config)

        self._metrics_out = open(
            Path(self._config.planner_log_path) / "actual_metrics.csv",
            "a",
            encoding="UTF-8",
        )
        self._scoring_out = open(
            Path(self._config.planner_log_path) / "scoring_out.csv",
            "a",
            encoding="UTF-8",
        )

    async def run_forever(self) -> None:
        try:
            while True:
                await asyncio.sleep(3)
                logger.debug("Planner is checking if a replan is needed...")
                if self._check_if_metrics_warrant_replanning():
                    await self._replan()
        finally:
            self._metrics_out.close()
            self._scoring_out.close()

    async def _replan(self) -> None:
        # This will be long-running and will block the event loop. For our
        # current needs, this is fine since the planner is the main component in
        # the daemon process.
        logger.info("Running a replan.")
        self._log_current_metrics()
        next_workload = self._expected_workload()
        workload_filters = [
            AuroraTransactions(next_workload),
            SingleEngineExecution(next_workload),
        ]

        # No need to keep around all candidates if we are selecting the best
        # blueprint. But for debugging purposes it is useful to see what
        # blueprints are being considered.
        candidate_set: List[_BlueprintCandidate] = []
        num_top = 50

        # Establish connections to the underlying engines (needed for scoring
        # purposes). We use synchronous connections since there appears to be a
        # bug in aioodbc that causes an indefinite await on a query result.
        engines = EngineConnections.connect_sync(
            self._config, self._schema_name, autocommit=False
        )
        table_sizer = TableSizer(engines, self._config)

        try:
            # Load metrics.
            metrics = self._monitor.read_k_most_recent(metric_ids=ALL_METRICS)

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

            for idx, bp in enumerate(
                NeighborhoodBlueprintEnumerator.enumerate(
                    self._current_blueprint,
                    self._planner_config.max_num_table_moves(),
                    self._planner_config.max_provisioning_multiplier(),
                )
            ):
                if idx % 10000 == 0:
                    logger.debug("Processing %d", idx)

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
                scoring_ctx.reset(bp)
                score = self._scorer.score(scoring_ctx)

                # Store the blueprint (for debugging purposes).
                if len(candidate_set) < num_top:
                    candidate_set.append(_BlueprintCandidate(bp.to_blueprint(), score))
                    if len(candidate_set) == num_top:
                        heapq.heapify(candidate_set)
                elif candidate_set[0].score_value > score.single_value():
                    # Replace the "worst" blueprint so far with this one (lower
                    # score is better).
                    latest = _BlueprintCandidate(bp.to_blueprint(), score)
                    heapq.heappushpop(candidate_set, latest)

            # Sort by score - lower is better.
            candidate_set.sort(key=lambda bpc: bpc.score_value)

            # Log the top 50 candidate plans.
            for candidate in candidate_set:
                logger.debug("%s", candidate.score)
                logger.debug("%s", candidate.blueprint)
                logger.debug("----------")

            if len(candidate_set) == 0:
                logger.error("Planner did not find any valid candidate blueprints.")
                logger.error("Next workload: %s", next_workload)
                raise RuntimeError("No valid candidates!")

            self._log_scoring_debug(candidate_set[0])
            best_blueprint = candidate_set[0].blueprint
            best_score = candidate_set[0].score
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
            current_engine = current_router.engine_for(q)
            q.populate_data_accessed_mb(
                current_engine, engines, self._current_blueprint
            )
            total_accessed_mb[current_engine] += q.data_accessed_mb(current_engine)

        return total_accessed_mb

    def _log_current_metrics(self) -> None:
        redshift_prov = self._current_blueprint.redshift_provisioning()
        aurora_prov = self._current_blueprint.aurora_provisioning()
        metrics = self._monitor.read_k_most_recent(metric_ids=ALL_METRICS)
        # Prepend provisioning information.
        metrics.insert(0, "redshift_instance_type", redshift_prov.instance_type())
        metrics.insert(1, "redshift_num_nodes", redshift_prov.num_nodes())
        metrics.insert(2, "aurora_instance_type", aurora_prov.instance_type())
        metrics.insert(3, "aurora_num_nodes", aurora_prov.num_nodes())
        string_csv = metrics.to_csv(index=False)
        self._metrics_out.write(string_csv)
        self._metrics_out.flush()

    def _log_scoring_debug(self, candidate: "_BlueprintCandidate") -> None:
        writer = csv.writer(self._scoring_out)
        cols = [
            "redshift_instance_type",
            "redshift_num_nodes",
            "aurora_instance_type",
            "aurora_num_nodes",
            *candidate.score.perf_debugging().keys(),
        ]
        redshift_prov = candidate.blueprint.redshift_provisioning()
        aurora_prov = candidate.blueprint.aurora_provisioning()
        values = [
            redshift_prov.instance_type(),
            redshift_prov.num_nodes(),
            aurora_prov.instance_type(),
            aurora_prov.num_nodes(),
        ]
        for col in cols[4:]:
            values.append(candidate.score.perf_debugging()[col])
        writer.writerow(cols)
        writer.writerow(values)


class _BlueprintCandidate:
    def __init__(self, blueprint: Blueprint, score: Score) -> None:
        self.blueprint = blueprint
        self.score = score
        self.score_value = score.single_value()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, _BlueprintCandidate):
            return False
        # N.B. We invert this __lt__ definition since we want to use it with
        # `heapq` to create a max-heap (highest score at index 0).
        return self.score_value > other.score_value
