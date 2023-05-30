from brad.blueprint import Blueprint
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.planner.neighborhood.score import ScoringContext


class NeighborhoodImpl:
    def on_start_enumeration(self) -> None:
        raise NotImplementedError

    def on_enumerated_blueprint(
        self, bp: EnumeratedBlueprint, ctx: ScoringContext
    ) -> None:
        raise NotImplementedError

    def on_enumeration_complete(self, ctx: ScoringContext) -> Blueprint:
        raise NotImplementedError
