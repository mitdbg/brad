import sqlite3
from typing import Optional

from .connection import Connection
from .cursor import Cursor
from .sqlite_cursor import SqliteCursor


class SqliteConnection(Connection):
    @classmethod
    def connect(cls, db_path: str, autocommit: bool) -> Connection:
        return cls.connect_sync(db_path, autocommit)

    @classmethod
    def connect_sync(cls, db_path: str, autocommit: bool) -> Connection:
        # Note in Python 3.12, the `autocommit` parameter becomes available.
        conn = sqlite3.connect(
            db_path, isolation_level=None if autocommit else "DEFERRED"
        )
        return cls(conn)

    def __init__(self, connection_impl: sqlite3.Connection) -> None:
        super().__init__()
        self._connection = connection_impl
        self._cursor: Optional[Cursor] = None
        self._is_closed = False

    async def cursor(self) -> Cursor:
        return self.cursor_sync()

    async def close(self) -> None:
        return self.close_sync()

    def cursor_sync(self) -> Cursor:
        if self._cursor is not None:
            return self._cursor
        self._cursor = SqliteCursor(self._connection, self._connection.cursor())
        return self._cursor

    def close_sync(self) -> None:
        if self._is_closed:
            return
        self._connection.close()
        self._is_closed = True
        self._cursor = None

    def is_connection_lost_error(self, ex: Exception) -> bool:
        return False

    def __del__(self) -> None:
        self.close_sync()
