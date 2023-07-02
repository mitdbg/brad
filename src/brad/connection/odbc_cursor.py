import asyncio
from typing import Any, Optional, List

from .cursor import Cursor, Row


class OdbcCursor(Cursor):
    def __init__(self, impl: Any) -> None:
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

    def execute_sync(self, query: str) -> None:
        self._impl.execute(query)

    def fetchone_sync(self) -> Optional[Row]:
        return self._impl.fetchone()

    def fetchall_sync(self) -> List[Row]:
        return self._impl.fetchall()

    def commit_sync(self) -> None:
        self._impl.commit()

    def rollback_sync(self) -> None:
        self._impl.rollback()
