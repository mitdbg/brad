import enum


class PlanningStrategy(str, enum.Enum):
    FullNeighborhood = "full_neighborhood"
    SampledNeighborhood = "sampled_neighborhood"
    QueryBasedBeam = "query_based_beam"

    @staticmethod
    def from_str(candidate: str) -> "PlanningStrategy":
        if candidate == PlanningStrategy.FullNeighborhood.value:
            return PlanningStrategy.FullNeighborhood
        elif candidate == PlanningStrategy.SampledNeighborhood.value:
            return PlanningStrategy.SampledNeighborhood
        else:
            raise ValueError("Unrecognized planning strategy {}".format(candidate))
