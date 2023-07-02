import asyncio
import redshift_connector
from typing import Optional, List

from .cursor import Cursor, Row


class SyncRedshiftCursor(Cursor):
    def __init__(self, impl: redshift_connector.Cursor) -> None:
        super().__init__()
        self._impl = impl

    def execute(self, query: str) -> None:
        self._impl.execute(query)

    def fetchone(self) -> Optional[Row]:
        return self._impl.fetchone()

    def fetchall(self) -> List[Row]:
        return self._impl.fetchall()

    def commit(self) -> None:
        self._impl.commit()

    def rollback(self) -> None:
        self._impl.rollback()


class AsyncRedshiftCursor(Cursor):
    def __init__(self, impl: redshift_connector.Cursor) -> None:
        super().__init__()
        self._impl = impl

    async def execute(self, query: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._impl.execute, query)

    async def fetchone(self) -> Optional[Row]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._impl.fetchone)

    async def fetchall(self) -> List[Row]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._impl.fetchall)

    async def commit(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._impl.commit)

    async def rollback(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._impl.rollback)
