import asyncio
import pyathena
import pyathena.connection
import pyathena.cursor
from typing import Optional, List

from .cursor import Cursor, Row


class PyAthenaCursor(Cursor):
    def __init__(self, impl: pyathena.cursor.Cursor) -> None:
        super().__init__()
        self._impl = impl

    async def execute(self, query: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._impl.execute, query)

    async def fetchone(self) -> Optional[Row]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._impl.fetchone)  # type: ignore

    async def fetchall(self) -> List[Row]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._impl.fetchall)  # type: ignore

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    def execute_sync(self, query: str) -> None:
        self._impl.execute(query)

    def fetchone_sync(self) -> Optional[Row]:
        return self._impl.fetchone()  # type: ignore

    def fetchall_sync(self) -> List[Row]:
        return self._impl.fetchall()  # type: ignore

    def commit_sync(self) -> None:
        pass

    def rollback_sync(self) -> None:
        pass
