from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from typing import Any, TypeAlias, final

from attrs import define, field
from typing_extensions import override

from brad.runner.generator import ImmutableGenerator, SqlGenerator
from brad.runner.schedule import Schedule, ScheduleGenerator
from brad.runner.user import User
from brad.runner.utils import to_tuple


@final
@define(frozen=True)
class Query:
    """
    Represents a SQL query with an execution schedule.

    In the context of a workload runner, a `Query` goes through
    the following lifecycle:
    - `INITIAL`: The query has not been processed by the runner yet.
    - `STARTED`: The .
    - `PENDING`: The runner has put the query into its internal queue.
    - `RUNNING`: The query has been sent to some database engine.
    - `COMPLETED`: The query has finished its execution.
    """

    sql: str
    schedule: Schedule


@final
@define(frozen=True)
class PendingQuery:
    """
    Represents a `Query` that has been scheduled by a workload runner
    to run at some time(s) in the future, but has not been executed yet
    (i.e. in the `PENDING` state).

    Definitions:
    - `processed_time` is the time when the query transitions from
      `INITIAL` to `STARTED`.
    """

    user: User
    query: Query
    processed_time: datetime

    def mark_complete(
        self, result: Sequence[Any], executed_time: datetime, execution_time: timedelta
    ) -> CompletedQuery:
        """Converts this query into a `CompletedQuery`."""
        return CompletedQuery(
            user=self.user,
            query=self.query,
            result=result,
            processed_time=self.processed_time,
            executed_time=executed_time,
            execution_time=execution_time,
        )


@final
@define(frozen=True)
class CompletedQuery:
    """
    Represents a `Query` that has completed its execution
    (i.e. in the `COMPLETED` state).

    Definitions:
    - `executed_time` is the time when the query transitions from
      `PENDING` to `RUNNING`.
    - `completed_time` is the time when the query transitions from
      `RUNNING` to `COMPLETED`.
    - `execution_time` is the duration between `executed_time` and
      `completed_time`.
    """

    user: User
    query: Query
    result: Sequence[Any] = field(converter=to_tuple)
    processed_time: datetime
    executed_time: datetime
    execution_time: timedelta

    @property
    def completed_time(self) -> datetime:
        return self.executed_time + self.execution_time


@final
@define(frozen=True)
class QueryGenerator(ImmutableGenerator[Query]):
    """
    Immutable generator that yields a (potentially unbounded) sequence of queries.
    """

    sql_generator: SqlGenerator
    schedule_generator: ScheduleGenerator

    @override
    def __iter__(self) -> Iterator[Query]:
        for sql, schedule in zip(self.sql_generator, self.schedule_generator):
            yield Query(sql, schedule)


# Avoid using `Iterable[Query]` since it complicates immutable design.
# Specifically, we can't simply defensive copy all iterables--what if they're unbounded?
Queries: TypeAlias = Sequence[Query] | ImmutableGenerator[Query]


@final
@define(frozen=True)
class ChainedQueries(ImmutableGenerator[Query]):
    """
    Represents an immutable `Queries` instance that consists of multiple
    subqueries.

    This class is intended to mimic the behavior of `itertools.chain()`,
    except that we need to implement our own chaining mechanism due to
    using `ImmutableGenerator` instead of normal iterables.
    """

    _all_queries: Sequence[Queries]

    def __init__(self, *all_queries: Queries) -> None:
        # Can't use direct assignment due to frozen dataclass
        object.__setattr__(
            self,
            "_all_queries",
            tuple(to_tuple(queries) for queries in all_queries),  # Defensive copy
        )

    @override
    def __iter__(self) -> Iterator[Query]:
        for queries in self._all_queries:
            yield from queries
