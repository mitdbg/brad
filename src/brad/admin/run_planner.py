import asyncio
import logging

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.monitor import Monitor
from brad.planner.neighborhood.full_neighborhood import FullNeighborhoodSearchPlanner
from brad.planner.neighborhood.sampled_neighborhood import (
    SampledNeighborhoodSearchPlanner,
)
from brad.planner.workload import Workload
from brad.planner.strategy import PlanningStrategy
from brad.server.blueprint_manager import BlueprintManager

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
        "--workload-dir",
        type=str,
        required=True,
        help="Path to the workload to load for planning purposes.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to run against.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    parser.set_defaults(admin_action=run_planner)


def run_planner(args):
    """
    This admin action is used to manually test the blueprint planner
    independently of the rest of BRAD.
    """
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Load the planner config.
    planner_config = PlannerConfig(args.planner_config_file)

    # 3. Load the blueprint.
    blueprint_mgr = BlueprintManager(config, args.schema_name)
    blueprint_mgr.load_sync()
    logger.info("Current blueprint:")
    logger.info("%s", blueprint_mgr.get_blueprint())

    # 4. Load the workload.
    workload = Workload.from_extracted_logs(args.workload_dir)

    # 5. Start the planner.
    monitor = Monitor(config)
    strategy = planner_config.strategy()
    if strategy == PlanningStrategy.FullNeighborhood:
        planner = FullNeighborhoodSearchPlanner(
            current_blueprint=blueprint_mgr.get_blueprint(),
            current_workload=workload,
            planner_config=planner_config,
            monitor=monitor,
            config=config,
            schema_name=args.schema_name,
        )
    elif strategy == PlanningStrategy.SampledNeighborhood:
        planner = SampledNeighborhoodSearchPlanner(
            current_blueprint=blueprint_mgr.get_blueprint(),
            current_workload=workload,
            planner_config=planner_config,
            monitor=monitor,
            config=config,
            schema_name=args.schema_name,
        )
    else:
        assert False
    monitor.force_read_metrics()

    async def on_new_blueprint(blueprint: Blueprint):
        logger.info("Selected new blueprint")
        logger.info("%s", blueprint)

    planner.register_new_blueprint_callback(on_new_blueprint)

    # 6. Trigger replanning.
    event_loop = asyncio.new_event_loop()
    event_loop.set_debug(enabled=args.debug)
    asyncio.set_event_loop(event_loop)
    asyncio.run(planner.run_replan())
