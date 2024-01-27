import asyncio
import pyodbc
from typing import Any, Optional

from .connection import Connection, ConnectionFailed
from .cursor import Cursor
from .odbc_cursor import OdbcCursor


class OdbcConnection(Connection):
    @classmethod
    async def connect(
        cls, connection_str: str, autocommit: bool, timeout_s: int
    ) -> Connection:
        loop = asyncio.get_running_loop()

        def make_connection():
            return pyodbc.connect(
                connection_str, autocommit=autocommit, timeout=timeout_s
            )

        try:
            connection = await loop.run_in_executor(None, make_connection)
            return cls(connection)
        except pyodbc.OperationalError as ex:
            raise ConnectionFailed() from ex

    @classmethod
    def connect_sync(
        cls, connection_str: str, autocommit: bool, timeout_s: int
    ) -> Connection:
        try:
            connection = pyodbc.connect(
                connection_str, autocommit=autocommit, timeout=timeout_s
            )
            return cls(connection)
        except pyodbc.OperationalError as ex:
            raise ConnectionFailed() from ex

    def __init__(self, connection_impl: Any) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None
        self._is_closed = False

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = OdbcCursor(cursor_impl)
        return self._cursor

    async def close(self) -> None:
        if self._is_closed:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)
        self._is_closed = True

    def cursor_sync(self) -> Cursor:
        if self._cursor is None:
            self._cursor = OdbcCursor(self._connection.cursor())
        return self._cursor

    def close_sync(self) -> None:
        if self._is_closed:
            return
        self._connection.close()
        self._is_closed = True

    def is_connection_lost_error(self, ex: Exception) -> bool:
        if isinstance(ex, pyodbc.Error) or isinstance(ex, pyodbc.OperationalError):
            err_code = ex.args[0]
            if err_code in _CONNECTION_LOST_ERR_CODES:
                return True

        # Unfortunately, there is no nice exception type. So we fall back to
        # substring search.
        message = repr(ex)
        for phrase in _CONNECTION_LOST_PHRASES:
            if phrase in message:
                return True

        return False

    def __del__(self) -> None:
        self.close_sync()


# Error code 25006 is used when running DML statements on a read replica. This
# happens during the transient period when we failover to a new Aurora primary.
# The solution is to re-connect, so we treat this as a lost connection.
_CONNECTION_LOST_ERR_CODES = ["08001", "57P02", "08S01", "25006"]


_CONNECTION_LOST_PHRASES = [
    "server closed the connection unexpectedly",
    "connection has been lost",
    "connection lost",
]
