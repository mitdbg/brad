import enum


class PlanningStrategy(str, enum.Enum):
    FullNeighborhood = "full_neighborhood"
    SampledNeighborhood = "sampled_neighborhood"
    QueryBasedBeam = "query_based_beam"
    TableBasedBeam = "table_based_beam"
    QueryBasedLegacyBeam = "query_based_legacy_beam"
    RandomBaseline = "random_baseline"
    GreedyBaseline = "greedy_baseline"
    ExhaustiveBaseline = "exhaustive_baseline"

    @staticmethod
    def from_str(candidate: str) -> "PlanningStrategy":
        if candidate == PlanningStrategy.FullNeighborhood.value:
            return PlanningStrategy.FullNeighborhood
        elif candidate == PlanningStrategy.SampledNeighborhood.value:
            return PlanningStrategy.SampledNeighborhood
        elif candidate == PlanningStrategy.QueryBasedBeam.value:
            return PlanningStrategy.QueryBasedBeam
        elif candidate == PlanningStrategy.TableBasedBeam.value:
            return PlanningStrategy.TableBasedBeam
        elif candidate == PlanningStrategy.QueryBasedLegacyBeam.value:
            return PlanningStrategy.QueryBasedLegacyBeam
        elif candidate == PlanningStrategy.RandomBaseline.value:
            return PlanningStrategy.RandomBaseline
        elif candidate == PlanningStrategy.GreedyBaseline.value:
            return PlanningStrategy.GreedyBaseline
        elif candidate == PlanningStrategy.ExhaustiveBaseline.value:
            return PlanningStrategy.ExhaustiveBaseline
        else:
            raise ValueError("Unrecognized planning strategy {}".format(candidate))
