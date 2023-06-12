import enum


class BlueprintFeasibility(enum.Enum):
    Unchecked = 0
    Infeasible = 1
    StructurallyFeasible = 2
    Feasible = 3
