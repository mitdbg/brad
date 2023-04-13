from abc import abstractmethod
from typing import Protocol, final
from typing_extensions import override

from workloads.runner.query import CompletedQuery


class QueryReporter(Protocol):
    @abstractmethod
    async def report(self, completed_query: CompletedQuery) -> None:
        raise NotImplementedError


@final
class PrintReporter(QueryReporter):
    @override
    async def report(
            self, completed_query: CompletedQuery, verbose: bool = False
    ) -> None:
        if verbose:
            print("Done executing query : ", end="")
            print(completed_query)
        else:
            print(
                " ".join(
                    [
                        f"{completed_query.user.label}:",
                        f'"{completed_query.query.sql}"',
                        f"({completed_query.completed_time})",
                    ]
                )
            )
