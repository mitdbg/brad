from typing import Optional

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.planner import PlannerConfig
from brad.daemon.system_event_logger import SystemEventLogger
from brad.planner.abstract import BlueprintPlanner
from brad.planner.beam.query_based import QueryBasedBeamPlanner
from brad.planner.beam.query_based_legacy import QueryBasedLegacyBeamPlanner
from brad.planner.beam.table_based import TableBasedBeamPlanner
from brad.planner.neighborhood.neighborhood import NeighborhoodSearchPlanner
from brad.planner.providers import BlueprintProviders
from brad.planner.scoring.score import Score
from brad.planner.strategy import PlanningStrategy


class BlueprintPlannerFactory:
    @staticmethod
    def create(
        config: ConfigFile,
        planner_config: PlannerConfig,
        schema_name: str,
        current_blueprint: Blueprint,
        current_blueprint_score: Optional[Score],
        providers: BlueprintProviders,
        system_event_logger: Optional[SystemEventLogger] = None,
    ) -> BlueprintPlanner:
        strategy = planner_config.strategy()
        if (
            strategy == PlanningStrategy.FullNeighborhood
            or strategy == PlanningStrategy.SampledNeighborhood
        ):
            return NeighborhoodSearchPlanner(
                config=config,
                planner_config=planner_config,
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                providers=providers,
                system_event_logger=system_event_logger,
            )

        elif strategy == PlanningStrategy.QueryBasedBeam:
            return QueryBasedBeamPlanner(
                config=config,
                planner_config=planner_config,
                schema_name=schema_name,
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                providers=providers,
                system_event_logger=system_event_logger,
            )

        elif strategy == PlanningStrategy.QueryBasedLegacyBeam:
            return QueryBasedLegacyBeamPlanner(
                config=config,
                planner_config=planner_config,
                schema_name=schema_name,
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                providers=providers,
                system_event_logger=system_event_logger,
            )

        elif strategy == PlanningStrategy.TableBasedBeam:
            return TableBasedBeamPlanner(
                config=config,
                planner_config=planner_config,
                schema_name=schema_name,
                current_blueprint=current_blueprint,
                current_blueprint_score=current_blueprint_score,
                providers=providers,
                system_event_logger=system_event_logger,
            )

        else:
            raise ValueError("Unsupported planning strategy: {}".format(str(strategy)))
