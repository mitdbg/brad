from .cursor import Cursor


class Connection:
    """
    A wrapper class used to hide the different connection implementations for
    each engine and to unify the async/sync interface. This class also helps
    with type checking.
    """

    def __init__(self) -> None:
        self._connected = True

    async def cursor(self) -> Cursor:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    def cursor_sync(self) -> Cursor:
        raise NotImplementedError

    def close_sync(self) -> None:
        raise NotImplementedError

    def is_connection_lost_error(self, ex: Exception) -> bool:
        """
        Return `True` if the exception represents a connection lost exception.
        Each `Connection` implementation may have its own way of indicating a
        lost connection.
        """
        raise NotImplementedError

    def mark_connection_lost(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected


class ConnectionFailed(Exception):
    """
    Used when an existing connection fails for any reason, or we failed to
    establish a connection to an underlying engine.
    """
