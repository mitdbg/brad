from typing import Any, Tuple, Optional, List


Row = Tuple[Any, ...]


class Cursor:
    """
    A wrapper class used to hide the different cursor implementations for
    each engine and to unify the async/sync interface. This class also helps
    with type checking.
    """

    async def execute(self, query: str) -> None:
        raise NotImplementedError

    async def fetchone(self) -> Optional[Row]:
        raise NotImplementedError

    async def fetchall(self) -> List[Row]:
        raise NotImplementedError

    async def commit(self) -> None:
        raise NotImplementedError

    async def rollback(self) -> None:
        raise NotImplementedError

    def execute_sync(self, query: str) -> None:
        raise NotImplementedError

    def fetchone_sync(self) -> Optional[Row]:
        raise NotImplementedError

    def fetchall_sync(self) -> List[Row]:
        raise NotImplementedError

    def commit_sync(self) -> None:
        raise NotImplementedError

    def rollback_sync(self) -> None:
        raise NotImplementedError
