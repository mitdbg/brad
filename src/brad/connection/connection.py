from .cursor import Cursor


class Connection:
    """
    A wrapper class used to hide the different connection implementations for
    each engine and to unify the async/sync interface. This class also helps
    with type checking.
    """

    async def cursor(self) -> Cursor:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    def cursor_sync(self) -> Cursor:
        raise NotImplementedError

    def close_sync(self) -> None:
        raise NotImplementedError
