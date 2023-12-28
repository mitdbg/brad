import asyncio
import logging
import pathlib
import pickle
from typing import Optional

from brad.planner.estimator import EstimatorProvider, Estimator
from brad.planner.recorded_run import RecordedPlanningRun


logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "run_sensitivity",
        help="Replay a recorded blueprint planning run for debugging.",
    )
    parser.add_argument(
        "--recorded-run",
        type=str,
        required=True,
        help="Path to the file containing the recorded planning run.",
    )
    parser.add_argument("--pred-change-frac", type=float, required=True)
    parser.add_argument("--affected-frac", type=float)
    parser.add_argument("--sweep-type", choices=["run_time", "scan_amount", "txn_lat"])
    parser.add_argument("--out-dir", type=str, default=".")
    parser.set_defaults(admin_action=run_sensitivity)


class _EstimatorProvider(EstimatorProvider):
    def __init__(self) -> None:
        self._estimator: Optional[Estimator] = None

    def set_estimator(self, estimator: Estimator) -> None:
        self._estimator = estimator

    def get_estimator(self) -> Optional[Estimator]:
        return self._estimator


async def run_sensitivity_impl(args) -> None:
    out_dir = pathlib.Path(args.out_dir)
    assert out_dir.exists(), "Output path does not exist."

    provider = _EstimatorProvider()
    recorded_run = RecordedPlanningRun.load(args.recorded_run)
    planner = recorded_run.create_planner(provider, args)

    logger.info("Re-running recorded run of type: %s", str(type(recorded_run)))
    result = await planner._run_replan_impl()  # pylint: disable=protected-access

    if result is None:
        with open(out_dir / "nothing", "w") as file:
            pass
    else:
        bp, score = result
        with open(out_dir / "blueprint.pkl", "wb") as file:
            pickle.dump(bp, file)
        with open(out_dir / "score.pkl", "wb") as file:
            file.write(score.serialize())


# This method is called by `brad.exec.admin.main`.
def run_sensitivity(args):
    asyncio.run(run_sensitivity_impl(args))
