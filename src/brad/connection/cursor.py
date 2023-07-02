from typing import Any, Tuple, Optional, List


Row = Tuple[Any, ...]


class Cursor:
    """
    A wrapper class used to hide the different cursor implementations for
    each engine and to unify the async/sync interface. This class also helps
    with type checking.
    """

    def execute(self, query: str) -> None:
        raise NotImplementedError

    def fetchone(self) -> Optional[Row]:
        raise NotImplementedError

    def fetchall(self) -> List[Row]:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError
