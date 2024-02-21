import logging
import sqlite3
from typing import Any, Optional, List, Iterable

from brad.connection.cursor import Cursor, Row

logger = logging.getLogger(__name__)


class SqliteCursor(Cursor):
    def __init__(
        self, conn_impl: sqlite3.Connection, cursor_impl: sqlite3.Cursor
    ) -> None:
        self._conn_impl = conn_impl
        self._cursor_impl = cursor_impl

    async def execute(self, query: str) -> None:
        return self.execute_sync(query)

    async def fetchone(self) -> Optional[Row]:
        return self.fetchone_sync()

    async def fetchall(self) -> List[Row]:
        return self.fetchall_sync()

    async def commit(self) -> None:
        return self.commit_sync()

    async def rollback(self) -> None:
        return self.rollback_sync()

    def execute_sync(self, query: str) -> None:
        if query.startswith("SET"):
            # HACK: To avoid invasive changes.
            logger.info("SqliteCursor: Skipping query %s", query)
            return
        self._cursor_impl.execute(query)

    def executemany_sync(self, query: str, batch: Iterable[Any]) -> None:
        self._cursor_impl.executemany(query, batch)

    def fetchone_sync(self) -> Optional[Row]:
        return self._cursor_impl.fetchone()

    def fetchall_sync(self) -> List[Row]:
        return self._cursor_impl.fetchall()

    def commit_sync(self) -> None:
        self._conn_impl.commit()

    def rollback_sync(self) -> None:
        self._conn_impl.rollback()
