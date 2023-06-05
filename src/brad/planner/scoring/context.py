from brad.blueprint import Blueprint
from brad.config.planner import PlannerConfig
from brad.planner.workload import Workload


class ScoringContext:
    """
    A wrapper class used to collect the components needed for blueprint scoring.
    """

    def __init__(
        self,
        current_blueprint: Blueprint,
        current_workload: Workload,
        next_workload: Workload,
        planner_config: PlannerConfig,
    ) -> None:
        self.current_blueprint = current_blueprint
        self.current_workload = current_workload
        self.next_workload = next_workload
        self.planner_config = planner_config
