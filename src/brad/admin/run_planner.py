import asyncio
import logging
import pathlib
import pytz
from typing import Dict
from datetime import timedelta, datetime

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.planner.compare.cost import best_cost_under_perf_ceilings
from brad.planner.estimator import EstimatorProvider, FixedEstimatorProvider
from brad.planner.factory import BlueprintPlannerFactory
from brad.planner.scoring.score import Score
from brad.planner.scoring.data_access.precomputed_values import (
    PrecomputedDataAccessProvider,
)
from brad.planner.scoring.performance.precomputed_predictions import (
    PrecomputedPredictions,
)
from brad.planner.metrics import (
    MetricsFromMonitor,
    FixedMetricsProvider,
    Metrics,
    MetricsProvider,
)
from brad.planner.workload import Workload
from brad.planner.workload.builder import WorkloadBuilder
from brad.planner.workload.provider import FixedWorkloadProvider
from brad.routing.policy import RoutingPolicy
from brad.blueprint.manager import BlueprintManager
from brad.front_end.engine_connections import EngineConnections
from brad.utils.table_sizer import TableSizer

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "run_planner", help="Run the BRAD blueprint planner for testing purposes."
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--planner-config-file",
        type=str,
        required=True,
        help="Path to the blueprint planner's configuration file.",
    )
    parser.add_argument(
        "--workload-source",
        type=str,
        required=True,
        help="Used to specify how to construct the workload to use.",
    )
    parser.add_argument(
        "--workload-dir",
        type=str,
        help="Path to the workload to load for planning purposes.",
    )
    parser.add_argument("--query-bank-file", type=str)
    parser.add_argument("--query-counts-file", type=str)
    parser.add_argument("--query-counts-multiplier", type=int, default=1)
    parser.add_argument(
        "--predictions-dir",
        type=str,
        required=True,
        help="Path to the workload's predicted statistics (execution times, data access).",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to run against.",
    )
    parser.add_argument(
        "--analytical-rate-per-s",
        type=float,
        help="The number of analytical queries issued per second.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.add_argument(
        "--latency-ceiling-s",
        type=float,
        default=10.0,
        help="The geomean latency ceiling to use for blueprint planning.",
    )
    parser.add_argument(
        "--use-fixed-metrics",
        type=str,
        help="If set, use comma-separated hardcoded metrics of the form 'metric_name=value'.",
    )
    parser.add_argument(
        "--save-pickle",
        action="store_true",
        help="If set, serialize the decorated workload.",
    )
    parser.set_defaults(admin_action=run_planner)


def parse_metrics(kv_str: str) -> Dict[str, float]:
    # `kv_str` contains comma-separated values of the form `metric_name=value`.
    pairs = kv_str.split(",")
    metrics = {}
    for p in pairs:
        kv = p.split("=")
        metrics[kv[0]] = float(kv[1])
    return metrics


async def run_planner_impl(args) -> None:
    """
    This admin action is used to manually test the blueprint planner
    independently of the rest of BRAD.
    """
    # 1. Load the config.
    config = ConfigFile.load(args.config_file)

    # 2. Load the planner config.
    planner_config = PlannerConfig(args.planner_config_file)

    # 3. Load the blueprint.
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    blueprint_mgr.load_sync()
    logger.info("Current blueprint:")
    logger.info("%s", blueprint_mgr.get_blueprint())

    # 4. Load the workload.
    if args.workload_source == "pickle":
        workload = Workload.from_pickle(
            pathlib.Path(args.workload_dir) / _PICKLE_FILE_NAME
        )

    elif args.workload_source == "query_bank":
        assert args.query_bank_file is not None
        assert args.query_counts_file is not None
        assert args.workload_dir is not None

        engines = EngineConnections.connect_sync(
            config, args.schema_name, autocommit=True
        )
        workload_dir = pathlib.Path(args.workload_dir)
        table_sizer = TableSizer(engines, config)
        builder = (
            WorkloadBuilder()
            .add_analytical_queries_and_counts_from_file(
                args.query_bank_file,
                args.query_counts_file,
            )
            .add_transactional_queries_from_file(workload_dir / "oltp.sql")
            .for_period(timedelta(hours=1))
        )
        workload = (
            await builder.table_sizes_from_engines(
                blueprint_mgr.get_blueprint(), table_sizer
            )
        ).build()

    elif args.workload_source == "workload_dir":
        assert args.analytical_rate_per_s is not None

        engines = EngineConnections.connect_sync(
            config, args.schema_name, autocommit=True
        )
        table_sizer = TableSizer(engines, config)
        workload_dir = pathlib.Path(args.workload_dir)
        builder = (
            WorkloadBuilder()
            .add_analytical_queries_from_file(workload_dir / "olap.sql")
            .add_transactional_queries_from_file(workload_dir / "oltp.sql")
            .uniform_per_analytical_query_rate(
                args.analytical_rate_per_s, period=timedelta(seconds=1)
            )
            .for_period(timedelta(hours=1))
        )
        workload = (
            await builder.table_sizes_from_engines(
                blueprint_mgr.get_blueprint(), table_sizer
            )
        ).build()

    # 5. Load the pre-computed predictions.
    prediction_dir = pathlib.Path(args.predictions_dir)
    prediction_provider = PrecomputedPredictions.load(
        workload_file_path=prediction_dir / "all_queries.sql",
        aurora_predictions_path=prediction_dir / "pred_aurora_runtime.npy",
        redshift_predictions_path=prediction_dir / "pred_redshift_runtime.npy",
        athena_predictions_path=prediction_dir / "pred_athena_runtime.npy",
    )
    data_access_provider = PrecomputedDataAccessProvider.load(
        workload_file_path=prediction_dir / "all_queries.sql",
        aurora_accessed_pages_path=prediction_dir
        / "all_queries_aurora_blocks_accessed.npy",
        athena_accessed_bytes_path=prediction_dir
        / "all_queries_athena_scanned_bytes.npy",
    )

    # 6. Start the planner.
    monitor = Monitor(config, blueprint_mgr)
    monitor.set_up_metrics_sources()
    if args.use_fixed_metrics is not None:
        now = datetime.now().astimezone(pytz.utc)
        metrics_provider: MetricsProvider = FixedMetricsProvider(
            Metrics(**parse_metrics(args.use_fixed_metrics)),
            now,
        )
    else:
        metrics_provider = MetricsFromMonitor(monitor, blueprint_mgr)

    if config.routing_policy == RoutingPolicy.ForestTableSelectivity:
        pe = asyncio.run(PostgresEstimator.connect(args.schema_name, config))
        asyncio.run(pe.analyze(blueprint_mgr.get_blueprint()))
        estimator_provider: EstimatorProvider = FixedEstimatorProvider(pe)
    else:
        estimator_provider = EstimatorProvider()

    planner = BlueprintPlannerFactory.create(
        current_blueprint=blueprint_mgr.get_blueprint(),
        current_blueprint_score=blueprint_mgr.get_active_score(),
        planner_config=planner_config,
        monitor=monitor,
        config=config,
        schema_name=args.schema_name,
        # Next workload is the same as the current workload.
        workload_provider=FixedWorkloadProvider(workload),
        # Used for debugging purposes.
        analytics_latency_scorer=prediction_provider,
        # TODO: Make this configurable.
        comparator=best_cost_under_perf_ceilings(
            max_query_latency_s=args.latency_ceiling_s,
            max_txn_p90_latency_s=0.020,  # FIXME: Add command-line argument if needed.
        ),
        metrics_provider=metrics_provider,
        data_access_provider=data_access_provider,
        estimator_provider=estimator_provider,
    )
    asyncio.run(monitor.fetch_latest())

    async def on_new_blueprint(blueprint: Blueprint, score: Score):
        logger.info("Selected new blueprint")
        logger.info("%s", blueprint)

        while True:
            response = input(
                "Do you want to persist this blueprint? Use 'f' to force-persist the blueprint. (y/f/n): "
            ).lower()
            if response == "y":
                await blueprint_mgr.start_transition(blueprint, score)
                print("Done!")
                break
            elif response == "f":
                print("Forcing the blueprint...")
                blueprint_mgr.force_new_blueprint_sync(blueprint, score)
                print("Done!")
                break
            elif response == "n":
                break
            else:
                print("Invalid input. Please enter 'y', 'f', or 'n'.")

    planner.register_new_blueprint_callback(on_new_blueprint)

    # 7. Trigger replanning.
    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=args.debug)
    asyncio.set_event_loop(event_loop)
    asyncio.run(planner.run_replan(trigger=None))

    if args.save_pickle:
        workload.serialize_for_debugging(
            pathlib.Path(args.workload_dir) / _PICKLE_FILE_NAME
        )


def run_planner(args) -> None:
    asyncio.run(run_planner_impl(args))


_PICKLE_FILE_NAME = "workload.pickle"
