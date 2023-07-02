import asyncio
import pyodbc
from typing import Any, Optional

from .connection import Connection
from .cursor import Cursor
from .odbc_cursor import SyncOdbcCursor, AsyncOdbcCursor


class SyncOdbcConnection(Connection):
    @classmethod
    def connect(cls, connection_str: str, autocommit: bool) -> Connection:
        connection = pyodbc.connect(connection_str, autocommit=autocommit)
        return cls(connection)

    def __init__(self, connection_impl: Any) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    def cursor(self) -> Cursor:
        if self._cursor is None:
            self._cursor = SyncOdbcCursor(self._connection.cursor())
        return self._cursor

    def close(self) -> None:
        self._connection.close()


class AsyncOdbcConnection(Connection):
    @classmethod
    async def connect(cls, connection_str: str, autocommit: bool) -> Connection:
        loop = asyncio.get_running_loop()

        def make_connection():
            return pyodbc.connect(connection_str, autocommit=autocommit)

        connection = await loop.run_in_executor(None, make_connection)
        return cls(connection)

    def __init__(self, connection_impl: Any) -> None:
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None

    async def cursor(self) -> Cursor:
        if self._cursor is None:
            loop = asyncio.get_running_loop()
            cursor_impl = await loop.run_in_executor(None, self._connection.cursor)
            self._cursor = AsyncOdbcCursor(cursor_impl)
        return self._cursor

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._connection.close)
