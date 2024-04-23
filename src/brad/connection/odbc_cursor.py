import asyncio
import datetime
import decimal
from typing import Any, Optional, List, Iterable

from .cursor import Cursor, Row
from .schema import Schema, Field, DataType


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
        res = await loop.run_in_executor(None, self._impl.fetchall)
        print("ODBC", self._impl.description)
        return res

    async def commit(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._impl.commit)

    async def rollback(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._impl.rollback)

    def execute_sync(self, query: str) -> None:
        self._impl.execute(query)

    def executemany_sync(self, query: str, batch: Iterable[Any]) -> None:
        self._impl.executemany(query, batch)

    def fetchone_sync(self) -> Optional[Row]:
        return self._impl.fetchone()

    def fetchall_sync(self) -> List[Row]:
        res = self._impl.fetchall()
        return res

    def result_schema(self) -> Schema:
        fields = []
        for column_metadata in self._impl.description:
            column_name = column_metadata[0]
            odbc_type = column_metadata[1]
            if odbc_type is int:
                brad_type = DataType.Integer
            elif odbc_type is str:
                brad_type = DataType.String
            elif odbc_type is float:
                brad_type = DataType.Float
            elif odbc_type is bool:
                brad_type = DataType.Integer
            elif odbc_type is decimal.Decimal:
                brad_type = DataType.Decimal
            elif odbc_type is datetime.datetime:
                brad_type = DataType.Timestamp
            else:
                brad_type = DataType.Unknown
            fields.append(Field(name=column_name, data_type=brad_type))
        return Schema(fields)

    def commit_sync(self) -> None:
        self._impl.commit()

    def rollback_sync(self) -> None:
        self._impl.rollback()
