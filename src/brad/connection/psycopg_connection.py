import asyncio
import psycopg
from typing import Optional

from .connection import Connection, ConnectionFailed
from .cursor import Cursor
from .psycopg_cursor import PsycopgCursor


class PsycopgConnection(Connection):
    @classmethod
    async def connect(cls, connection_str: str, autocommit: bool) -> Connection:
        loop = asyncio.get_running_loop()

        def make_connection():
            return psycopg.connect(connection_str, autocommit=autocommit)

        try:
            connection = await loop.run_in_executor(None, make_connection)
            return cls(connection)
        except psycopg.OperationalError as ex:
            raise ConnectionFailed() from ex

    @classmethod
    def connect_sync(cls, connection_str: str, autocommit: bool) -> Connection:
        try:
            connection = psycopg.connect(connection_str, autocommit=autocommit)
            return cls(connection)
        except psycopg.OperationalError as ex:
            raise ConnectionFailed() from ex

    def __init__(self, connection_impl: psycopg.Connection) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    async def cursor(self) -> Cursor:
        return self.cursor_sync()

    async def close(self) -> None:
        self._connection.close()

    def cursor_sync(self) -> Cursor:
        if self._cursor is None:
            self._cursor = PsycopgCursor(self._connection, self._connection.cursor())
        return self._cursor

    def close_sync(self) -> None:
        self._connection.close()

    def is_connection_lost_error(self, ex: Exception) -> bool:
        if isinstance(ex, psycopg.Error) or isinstance(ex, psycopg.OperationalError):
            err_code = ex.sqlstate
            if err_code in _CONNECTION_LOST_ERR_CODES:
                return True

        # Unfortunately, there is no nice exception type. So we fall back to
        # substring search.
        message = repr(ex)
        for phrase in _CONNECTION_LOST_PHRASES:
            if phrase in message:
                return True

        return False


# Error code 25006 is used when running DML statements on a read replica. This
# happens during the transient period when we failover to a new Aurora primary.
# The solution is to re-connect, so we treat this as a lost connection.
# https://www.psycopg.org/psycopg3/docs/api/errors.html#sqlstate-exceptions
_CONNECTION_LOST_ERR_CODES = [
    "08001",
    "57P02",
    "08S01",
    "25006",
    "08000",
    "08003",
    "08004",
    "08006",
    "08007",
    "08P01",
]


_CONNECTION_LOST_PHRASES = [
    "server closed the connection unexpectedly",
    "connection has been lost",
    "connection lost",
]
