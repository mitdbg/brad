import enum
import logging
import numpy as np
import random
import math
import sys
from typing import Dict, List, Optional
from datetime import timedelta

from brad.config.engine import Engine
from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.metrics import Metrics
from brad.planner.workload import Workload
from brad.routing.router import Router
from brad.planner.scoring.provisioning import compute_athena_scan_cost_numpy
from brad.planner.scoring.provisioning import (
    compute_aurora_hourly_operational_cost,
    compute_redshift_hourly_operational_cost,
)
from brad.planner.scoring.performance.unified_aurora import AuroraProvisioningScore
from brad.planner.scoring.performance.unified_redshift import RedshiftProvisioningScore

logger = logging.getLogger(__name__)


class ExperimentKind(enum.Enum):
    Nothing = "nothing"
    RunTime = "run_time"
    ScanAmount = "scan_amount"
    TxnLatency = "txn_lat"


class ScoringContext:
    """
    A wrapper class used to collect the components needed for blueprint scoring.
    """

    def __init__(
        self,
        schema_name: str,
        current_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        metrics: Metrics,
        planner_config: PlannerConfig,
    ) -> None:
        self.schema_name = schema_name
        self.current_blueprint = current_blueprint
        self.current_workload = current_workload
        self.next_workload = next_workload
        self.metrics = metrics
        self.planner_config = planner_config

        self.current_query_locations: Dict[Engine, List[int]] = {}
        self.current_query_locations[Engine.Aurora] = []
        self.current_query_locations[Engine.Redshift] = []
        self.current_query_locations[Engine.Athena] = []

        # TODO: This is messy - we should have one place for blueprint scoring
        # relative to a workload.
        self.current_workload_predicted_hourly_scan_cost = 0.0
        self.current_blueprint_provisioning_hourly_cost = 0.0

        # This is used for reweighing metrics due to query routing changes
        # across blueprints.
        self.engine_latency_norm_factor: Dict[Engine, float] = {}

        self.already_logged_txn_interference_warning = False

        # Used to memoize this value instead of recomputing it as it is a
        # function of the CPU utilization values.
        self.cpu_skew_adjustment: Optional[float] = None

        self.exp_kind = ExperimentKind.Nothing
        self.exp_change_frac = 0.0
        self.exp_affected_queries: List[int] = []

    def set_up_sensitivity_state(self, args) -> None:
        if args.exp_kind == ExperimentKind.RunTime.value:
            self.exp_kind = ExperimentKind.RunTime
        elif args.exp_kind == ExperimentKind.ScanAmount.value:
            self.exp_kind = ExperimentKind.ScanAmount
        elif args.exp_kind == ExperimentKind.TxnLatency.value:
            self.exp_kind = ExperimentKind.TxnLatency
        else:
            raise AssertionError("Unknown: " + args.exp_kind)

        # Add 1 so we can just multiply this value by the predictions.
        self.exp_change_frac = 1.0 + args.pred_change_frac
        print(
            "Running {} with change frac {:.4f}".format(
                str(self.exp_kind), self.exp_change_frac
            ),
            file=sys.stderr,
            flush=True,
        )

        if (
            self.exp_kind == ExperimentKind.RunTime
            or self.exp_kind == ExperimentKind.ScanAmount
        ):
            prng = random.Random(args.seed)
            assert args.affected_frac is not None
            num_queries = len(self.next_workload.analytical_queries())
            num_affected = math.ceil(args.affected_frac * num_queries)
            self.exp_affected_queries = prng.sample(range(num_queries), k=num_affected)
            print(
                "Affected queries:",
                self.exp_affected_queries,
                file=sys.stderr,
                flush=True,
            )

        if self.exp_kind == ExperimentKind.RunTime:
            assert self.next_workload._predicted_analytical_latencies is not None
            # We scale Athena here because it is not provisioning dependent.
            self.next_workload._predicted_analytical_latencies[
                self.exp_affected_queries, Workload.EngineLatencyIndex[Engine.Athena]
            ] *= self.exp_change_frac

        if self.exp_kind == ExperimentKind.ScanAmount:
            assert self.next_workload._predicted_athena_bytes_accessed is not None
            # Increase the scan amount.
            self.next_workload._predicted_athena_bytes_accessed[
                self.exp_affected_queries
            ] *= self.exp_change_frac

    async def simulate_current_workload_routing(self, router: Router) -> None:
        self.current_query_locations[Engine.Aurora].clear()
        self.current_query_locations[Engine.Redshift].clear()
        self.current_query_locations[Engine.Athena].clear()

        use_recorded_routing_if_available = self.planner_config.flag(
            "use_recorded_routing_if_available", default=False
        )

        all_queries = self.current_workload.analytical_queries()
        for qidx, query in enumerate(all_queries):
            if use_recorded_routing_if_available:
                maybe_eng = query.most_recent_execution_location()
                if maybe_eng is not None:
                    self.current_query_locations[maybe_eng].append(qidx)
                    continue

            # Fall back to the router if the historical routing location is not
            # available.
            eng = await router.engine_for(query)
            self.current_query_locations[eng].append(qidx)

    def compute_current_workload_predicted_hourly_scan_cost(self) -> None:
        # Note that this is not ideal. We should use the actual _recorded_ scan
        # cost (but this is not readily recorded through PyAthena).
        # Athena bytes:
        bytes_accessed = (
            self.current_workload.get_predicted_athena_bytes_accessed_batch(
                self.current_query_locations[Engine.Athena]
            )
        )
        arrival_counts = self.current_workload.get_arrival_counts_batch(
            self.current_query_locations[Engine.Athena]
        )
        period_scan_cost = compute_athena_scan_cost_numpy(
            bytes_accessed, arrival_counts, self.planner_config
        )
        scaling_factor = (
            timedelta(hours=1).total_seconds()
            / self.current_workload.period().total_seconds()
        )
        self.current_workload_predicted_hourly_scan_cost = (
            period_scan_cost * scaling_factor
        )
        if not self.planner_config.use_io_optimized_aurora():
            logger.warning("Aurora blocks accessed is not implemented.")

    def compute_current_blueprint_provisioning_hourly_cost(self) -> None:
        aurora_cost = compute_aurora_hourly_operational_cost(
            self.current_blueprint.aurora_provisioning(), self
        )
        redshift_cost = compute_redshift_hourly_operational_cost(
            self.current_blueprint.redshift_provisioning()
        )
        self.current_blueprint_provisioning_hourly_cost = aurora_cost + redshift_cost

    def compute_engine_latency_norm_factor(self) -> None:
        for engine in [Engine.Aurora, Engine.Redshift, Engine.Athena]:
            if len(self.current_query_locations[engine]) == 0:
                # Avoid having an explicit entry for engines that receive no
                # queries (the engine could be off).
                continue

            if (
                engine == Engine.Redshift
                and self.current_blueprint.redshift_provisioning().num_nodes() == 0
            ):
                # Avoid having an entry for engines that are off. The check
                # above might pass if we are using a long planning window and
                # queries executed on Redshift in the past.
                continue

            if (
                engine == Engine.Aurora
                and self.current_blueprint.aurora_provisioning().num_nodes() == 0
            ):
                # Avoid having an entry for engines that are off. The check
                # above might pass if we are using a long planning window and
                # queries executed on Aurora in the past.
                continue

            all_queries = self.current_workload.analytical_queries()
            relevant_queries = []
            for qidx in self.current_query_locations[engine]:
                relevant_queries.append(all_queries[qidx])

            # 1. Get predicted base latencies.
            predicted_base_latencies = (
                self.current_workload.get_predicted_analytical_latency_batch(
                    self.current_query_locations[engine], engine
                )
            )

            # 2. Adjust to the provisioning if needed.
            if engine == Engine.Aurora:
                adjusted_latencies = (
                    AuroraProvisioningScore.predict_query_latency_resources(
                        predicted_base_latencies,
                        self.current_blueprint.aurora_provisioning(),
                        self,
                    )
                )
            elif engine == Engine.Redshift:
                adjusted_latencies = (
                    RedshiftProvisioningScore.predict_query_latency_resources(
                        predicted_base_latencies,
                        self.current_blueprint.redshift_provisioning(),
                        self,
                    )
                )
            elif engine == Engine.Athena:
                # No provisioning.
                adjusted_latencies = predicted_base_latencies

            # 3. Extract query weights (based on arrival frequency) and scale
            # the run times.
            query_weights = self.current_workload.get_arrival_counts_batch(
                self.current_query_locations[engine]
            )
            assert query_weights.shape == predicted_base_latencies.shape

            self.engine_latency_norm_factor[engine] = np.dot(
                adjusted_latencies, query_weights
            )
