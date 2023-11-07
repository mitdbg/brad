import pathlib
import pickle

from brad.planner.abstract import BlueprintPlanner
from brad.planner.estimator import EstimatorProvider


class RecordedPlanningRun:
    """
    Represents a blueprint planning invocation. This is used for debugging
    purposes (e.g., to record a blueprint planning run and to replay it to
    understand why certain decisions were made).

    Concrete instances of this class must be pickle-able.
    """

    @staticmethod
    def load(file_path: pathlib.Path) -> "RecordedPlanningRun":
        with open(file_path, "rb") as file:
            return pickle.load(file)

    def serialize(self, file_path: pathlib.Path) -> None:
        with open(file_path, "wb") as file:
            pickle.dump(self, file)

    def create_planner(self, estimator_provider: EstimatorProvider) -> BlueprintPlanner:
        """
        Creates a `BlueprintPlanner` that will always run the recorded blueprint
        planning instance when `_run_replan_impl()` is called.

        Note that you must pass in an estimator provider because it relies on
        external state that cannot be serialized.

        The behavior of other planning methods is undefined (i.e., this is not
        meant to be a planner serializer).
        """
        raise NotImplementedError
