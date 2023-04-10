import sys
from collections import deque
from typing import List, Dict

from brad.config.engine import Engine


class LogicalDataSyncOperator:
    def __init__(self, table_name: str, engine: Engine) -> None:
        self._dependees: List["LogicalDataSyncOperator"] = []
        self._table_name = table_name
        self._engine = engine

        # Used to prune parts of the plan that we are sure will not produce any
        # deltas. This happens when a table has not had any changes.
        self._definitely_empty = False

    def table_name(self) -> str:
        return self._table_name

    def engine(self) -> Engine:
        """The engine associated with this operator."""
        return self._engine

    def add_dependee(self, dependee: "LogicalDataSyncOperator") -> None:
        self._dependees.append(dependee)

    def dependees(self) -> List["LogicalDataSyncOperator"]:
        """These are upstream operators that depend on this operator's outputs."""
        return self._dependees

    def dependencies(self) -> List["LogicalDataSyncOperator"]:
        raise NotImplementedError

    def is_definitely_empty(self) -> bool:
        return self._definitely_empty

    def set_definitely_empty(self, definitely_empty: bool) -> None:
        self._definitely_empty = definitely_empty


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

    def reset_definitely_empty(self) -> None:
        for op in self._operators:
            op.set_definitely_empty(False)

    def propagate_definitely_empty(self) -> None:
        """
        Propagates the "definitely empty" markers upwards from the base
        operators.

        If all of an operator's dependencies are "definitely empty" (and it has
        at least one dependency), then that operator will be empty too.
        """
        visited = set()

        def visit(op: LogicalDataSyncOperator) -> None:
            if op in visited:
                return
            visited.add(op)
            for dep in op.dependencies():
                visit(dep)
            if len(op.dependencies()) > 0 and all(
                map(lambda dep: dep.is_definitely_empty(), op.dependencies())
            ):
                op.set_definitely_empty(True)

        for op in self._operators:
            if len(op.dependees()) > 0:
                continue
            visit(op)

    def prune_empty_ops(self) -> "LogicalDataSyncPlan":
        """
        Returns a new plan with the subtrees that produce no deltas removed.
        This plan can be empty!

        This should only be called on a logical plan that was generated from the
        blueprint and that has had `propagate_definitely_empty()` called.
        """

        # A map from old operator to new operator.
        new_ops: Dict[LogicalDataSyncOperator, LogicalDataSyncOperator] = {}

        def process(op: LogicalDataSyncOperator) -> LogicalDataSyncOperator:
            if op in new_ops:
                return new_ops[op]

            if op.is_definitely_empty():
                new_op: LogicalDataSyncOperator = EmptyDeltas(
                    op.table_name(), op.engine()
                )
                new_ops[op] = new_op
                return new_op

            new_deps = []
            for dep in op.dependencies():
                new_deps.append(process(dep))

            if isinstance(op, ApplyDeltas):
                assert len(new_deps) == 1
                new_op = ApplyDeltas(new_deps[0], op.table_name(), op.engine())
            elif isinstance(op, TransformDeltas):
                new_op = TransformDeltas(
                    new_deps, op.transform_text(), op.table_name(), op.engine()
                )
            elif isinstance(op, ExtractDeltas):
                assert len(new_deps) == 0
                new_op = ExtractDeltas(op.table_name())
            else:
                # `EmptyDeltas` are not meant to be possible here.
                raise AssertionError

            new_ops[op] = new_op
            return new_op

        for op in self._operators:
            if len(op.dependees()) > 0:
                continue
            process(op)

        relevant_new_ops = []
        new_base_ops = []

        for nop in new_ops.values():
            if isinstance(nop, EmptyDeltas) and len(nop.dependees()) == 0:
                # Filter out top-level `EmptyDeltas`
                continue
            relevant_new_ops.append(nop)
            if len(nop.dependencies()) == 0:
                new_base_ops.append(nop)

        return LogicalDataSyncPlan(relevant_new_ops, new_base_ops)

    def print_plan_sequentially(self, file=sys.stdout) -> None:
        """
        Prints a topological ordering of the plan. Useful for debugging
        purposes.
        """

        deps_left = {op: len(op.dependencies()) for op in self._operators}
        ready_to_run = deque([*self._base_operators])

        print("Logical Data Sync Plan:", file=file)
        while len(ready_to_run) > 0:
            op = ready_to_run.popleft()
            print("-", str(op), file=file)
            for dependee in op.dependees():
                deps_left[dependee] -= 1
                if deps_left[dependee] == 0:
                    ready_to_run.append(dependee)


class ExtractDeltas(LogicalDataSyncOperator):
    """
    Extract the deltas from the specified table.

    The location is implicitly `Engine.Aurora` since we only support delta
    extraction on Aurora.
    """

    def __init__(self, table_name: str) -> None:
        super().__init__(table_name, Engine.Aurora)

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return []

    def __repr__(self) -> str:
        return "".join(["ExtractDeltas(", self._table_name, ")"])


class TransformDeltas(LogicalDataSyncOperator):
    """
    Transform the deltas from the given sources using the specified
    transformation. Run the transformation on the specified location.
    """

    def __init__(
        self,
        sources: List[LogicalDataSyncOperator],
        transform_text: str,
        table_name: str,
        engine: Engine,
    ):
        super().__init__(table_name, engine)
        self._sources = sources
        self._transform_text = transform_text

        for s in self._sources:
            # Sanity check.
            assert (
                isinstance(s, ExtractDeltas)
                or isinstance(s, TransformDeltas)
                or isinstance(s, EmptyDeltas)
            )
            s.add_dependee(self)

    def transform_text(self) -> str:
        return self._transform_text

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return self._sources

    def __repr__(self) -> str:
        return "".join(
            [
                "TransformDeltas(num_sources=",
                str(len(self._sources)),
                ", engine=",
                self.engine(),
                ")",
            ]
        )


class ApplyDeltas(LogicalDataSyncOperator):
    """
    Apply the deltas from the given source to the specified table in the
    specified location.
    """

    def __init__(
        self, source: LogicalDataSyncOperator, table_name: str, location: Engine
    ):
        super().__init__(table_name, location)
        self._source = source
        self._location = location

        # Sanity check.
        assert isinstance(source, ExtractDeltas) or isinstance(source, TransformDeltas)

        self._source.add_dependee(self)

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return [self._source]

    def __repr__(self) -> str:
        return "".join(
            ["ApplyDeltas(", str(self._table_name), ", location=", self._location, ")"]
        )


class EmptyDeltas(LogicalDataSyncOperator):
    """
    Used as a placeholder for a table that we know will not have any deltas.
    """

    def dependencies(self) -> List[LogicalDataSyncOperator]:
        return []

    def __repr__(self) -> str:
        return "".join(
            ["EmptyDeltas(", str(self._table_name), ", engine=", self.engine(), ")"]
        )
