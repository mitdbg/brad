import asyncio
import pyathena
import pyathena.connection
import pyathena.cursor
from typing import Any, Iterable, Optional, List

from .cursor import Cursor, Row
from .schema import Schema, Field, DataType


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
        res = await loop.run_in_executor(None, self._impl.fetchall)  # type: ignore
        print("Athena", self._impl.description)
        return res

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    def execute_sync(self, query: str) -> None:
        self._impl.execute(query)

    def executemany_sync(self, query: str, batch: Iterable[Any]) -> None:
        raise RuntimeError("Not supported on Athena.")

    def fetchone_sync(self) -> Optional[Row]:
        return self._impl.fetchone()  # type: ignore

    def fetchall_sync(self) -> List[Row]:
        res = self._impl.fetchall()  # type: ignore
        return res

    def result_schema(self) -> Schema:
        fields = []
        for column_metadata in self._impl.description:
            column_name = column_metadata[0]
            athena_type = column_metadata[1]
            if athena_type == "integer":
                brad_type = DataType.Integer
            elif athena_type == "varchar":
                brad_type = DataType.String
            elif athena_type == "float":
                brad_type = DataType.Float
            elif athena_type == "timestamp":
                brad_type = DataType.Timestamp
            elif athena_type == "decimal":
                brad_type = DataType.Decimal
            else:
                brad_type = DataType.Unknown
            fields.append(Field(name=column_name, data_type=brad_type))
        return Schema(fields)

    def commit_sync(self) -> None:
        pass

    def rollback_sync(self) -> None:
        pass
