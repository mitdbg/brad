from typing import Callable
from .blueprint import ComparableBlueprint


# A `BlueprintComparator` should return true iff the blueprint provided in the
# first argument is "better" than the blueprint provided in the second argument.
BlueprintComparator = Callable[[ComparableBlueprint, ComparableBlueprint], bool]
