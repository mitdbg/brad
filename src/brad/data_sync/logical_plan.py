from typing import List

from brad.blueprint.data.location import Location
from brad.blueprint.data.table import TableName


class LogicalDataSyncOperator:
    def __init__(self) -> None:
        self._dependees: List["LogicalDataSyncOperator"] = []

    def add_dependee(self, dependee: "LogicalDataSyncOperator") -> None:
        self._dependees.append(dependee)

    def dependees(self) -> List["LogicalDataSyncOperator"]:
        """These are upstream operators that depend on this operator's outputs."""
        return self._dependees

    def dependencies(self) -> List["LogicalDataSyncOperator"]:
        raise NotImplementedError


class LogicalDataSyncPlan:
    def __init__(
        self,
        operators: List[LogicalDataSyncOperator],
        base_operators: List[LogicalDataSyncOperator],
    ):
        self._operators = operators
        self._base_operators = base_operators

    def operators(self) -> List[LogicalDataSyncOperator]:
        return self._operators

    def base_operators(self) -> List[LogicalDataSyncOperator]:
        return self._base_operators


class ExtractDeltas(LogicalDataSyncOperator):
    """
    Extract the deltas from the specified table.

    The location is implicitly `Location.Aurora` since we only support delta
    extraction on Aurora.
    """

    def __init__(self, table_name: TableName):
        super().__init__()
        self._table_name = table_name

    def table_name(self) -> TableName:
        return self._table_name

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return []

    def __repr__(self) -> str:
        return "".join(["ExtractDeltas(", str(self._table_name), ")"])


class TransformDeltas(LogicalDataSyncOperator):
    """
    Transform the deltas from the given sources using the specified
    transformation. Run the transformation on the specified location.
    """

    def __init__(
        self,
        sources: List[LogicalDataSyncOperator],
        transform_text: str,
        location: Location,
    ):
        super().__init__()
        self._sources = sources
        self._transform_text = transform_text
        self._location = location

        for s in self._sources:
            # Sanity check.
            assert isinstance(s, ExtractDeltas) or isinstance(s, TransformDeltas)
            s.add_dependee(self)

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return self._sources

    def __repr__(self) -> str:
        return "".join(
            [
                "TransformDeltas(num_sources=",
                str(len(self._sources)),
                ", location=",
                self._location,
                ")",
            ]
        )


class ApplyDeltas(LogicalDataSyncOperator):
    """
    Apply the deltas from the given source to the specified table in the
    specified location.
    """

    def __init__(
        self, source: LogicalDataSyncOperator, table_name: TableName, location: Location
    ):
        super().__init__()
        self._source = source
        self._table_name = table_name
        self._location = location

        # Sanity check.
        assert isinstance(source, ExtractDeltas) or isinstance(source, TransformDeltas)

        self._source.add_dependee(self)

    def table_name(self) -> TableName:
        return self._table_name

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return [self._source]

    def __repr__(self) -> str:
        return "".join(
            ["ApplyDeltas(", str(self._table_name), ", location=", self._location, ")"]
        )
