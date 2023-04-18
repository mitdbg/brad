from abc import abstractmethod
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Protocol, Self, TypeVar, final

_Row_co = TypeVar("_Row_co", covariant=True)


class AsyncClient(Protocol[_Row_co]):
    """
    Database client interface

    Usage:
    ```
    with AsyncClientImpl(...) as client:
        async for row in client.execute("SELECT 1"):
            ...
    ```
    """

    @final
    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    @final
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Ignore the arguments, i.e. no need to handle/suppress exception
        await self.close()

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def execute(self, query: str) -> AsyncIterator[_Row_co]:
        """Execute a single statement."""
        # Use `yield` keyword to let the type checker know it is an async generator
        # See https://stackoverflow.com/a/68911014/21451742
        yield  # type: ignore
        raise NotImplementedError
