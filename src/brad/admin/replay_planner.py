import asyncio
import logging
from typing import Optional

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.file import ConfigFile
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.planner.estimator import EstimatorProvider, Estimator
from brad.planner.recorded_run import RecordedPlanningRun


logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "replay_planner",
        help="Replay a recorded blueprint planning run for debugging.",
    )
    parser.add_argument(
        "--system-config-file",
        type=str,
        required=True,
        help="Path to BRAD's system configuration file.",
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The schema name to use.",
    )
    parser.add_argument(
        "--recorded-run",
        type=str,
        required=True,
        help="Path to the file containing the recorded planning run.",
    )
    parser.add_argument(
        "--ignore-estimator",
        action="store_true",
        help="If set, do not attempt to set up the estimator.",
    )
    parser.set_defaults(admin_action=replay_planner)


class _EstimatorProvider(EstimatorProvider):
    def __init__(self) -> None:
        self._estimator: Optional[Estimator] = None

    def set_estimator(self, estimator: Estimator) -> None:
        self._estimator = estimator

    def get_estimator(self) -> Optional[Estimator]:
        return self._estimator


async def replay_planner_impl(args) -> None:
    config = ConfigFile.load_from_new_configs(
        phys_config=args.physical_config_file, system_config=args.system_config_file
    )

    provider = _EstimatorProvider()
    if not args.ignore_estimator:
        assets = AssetManager(config)
        blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
        await blueprint_mgr.load()
        estimator = await PostgresEstimator.connect(args.schema_name, config)
        await estimator.analyze(blueprint_mgr.get_blueprint())
        provider.set_estimator(estimator)

    recorded_run = RecordedPlanningRun.load(args.recorded_run)
    planner = recorded_run.create_planner(provider, None)

    logger.info("Re-running recorded run of type: %s", str(type(recorded_run)))
    await planner._run_replan_impl()  # pylint: disable=protected-access


# This method is called by `brad.exec.admin.main`.
def replay_planner(args):
    asyncio.run(replay_planner_impl(args))
