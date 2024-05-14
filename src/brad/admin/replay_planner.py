import asyncio
import logging
import math
from typing import Optional

import conductor.lib as cond
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
    parser.add_argument("--beam-size", type=int, required=True)
    parser.set_defaults(admin_action=replay_planner)


class _EstimatorProvider(EstimatorProvider):
    def __init__(self) -> None:
        self._estimator: Optional[Estimator] = None

    def set_estimator(self, estimator: Estimator) -> None:
        self._estimator = estimator

    def get_estimator(self) -> Optional[Estimator]:
        return self._estimator


async def replay_planner_impl(args) -> None:
    provider = _EstimatorProvider()
    recorded_run = RecordedPlanningRun.load(args.recorded_run)
    planner = recorded_run.create_planner(provider)

    logger.info("Re-running recorded run of type: %s", str(type(recorded_run)))
    result = await planner._run_replan_impl(1, args.beam_size)  # pylint: disable=protected-access

    out_dir = cond.get_output_path()
    with open(out_dir / "result.csv", "w", encoding="UTF-8") as file:
        print("beam_size,cost_score", file=file)
        score = math.nan
        if result is not None:
            _, _, score_debug = result
            try:
                score = score_debug["cost_score"]
            except KeyError:
                pass
        print(f"{args.beam_size},{score}", file=file)


# This method is called by `brad.exec.admin.main`.
def replay_planner(args):
    asyncio.run(replay_planner_impl(args))
