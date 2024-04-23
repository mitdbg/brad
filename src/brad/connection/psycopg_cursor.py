import asyncio
import psycopg
from typing import Any, Optional, List, Iterable

from .cursor import Cursor, Row
from .schema import Schema, Field, DataType


class PsycopgCursor(Cursor):
    def __init__(self, conn: psycopg.Connection, impl: psycopg.Cursor) -> None:
        super().__init__()
        self._conn = conn
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
        await loop.run_in_executor(None, self._conn.commit)

    async def rollback(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._conn.rollback)

    def execute_sync(self, query: str) -> None:
        self._impl.execute(query)

    def executemany_sync(self, query: str, batch: Iterable[Any]) -> None:
        self._impl.executemany(query, batch)

    def fetchone_sync(self) -> Optional[Row]:
        return self._impl.fetchone()

    def fetchall_sync(self) -> List[Row]:
        return self._impl.fetchall()

    def result_schema(self, results: Optional[List[Row]] = None) -> Schema:
        if self._impl.description is None:
            return Schema.empty()

        fields = []
        for column_metadata in self._impl.description:
            try:
                brad_type = _POSTGRESQL_OID_TO_BRAD_TYPE[column_metadata.type_code]
            except KeyError:
                brad_type = DataType.Unknown
            fields.append(Field(name=column_metadata.name, data_type=brad_type))
        return Schema(fields)

    def commit_sync(self) -> None:
        self._conn.commit()

    def rollback_sync(self) -> None:
        self._conn.rollback()


# Use iter(self._impl.adapters.types) to retrieve the types supported by the
# underlying database.
_POSTGRESQL_OID_TO_BRAD_TYPE = {
    # Integer types.
    16: DataType.Integer,  # bool
    21: DataType.Integer,  # int2
    23: DataType.Integer,  # int4
    20: DataType.Integer,  # int8
    26: DataType.Integer,  # oid
    # Float types.
    700: DataType.Float,  # float4
    701: DataType.Float,  # float8
    # Fixed precision types.
    1700: DataType.Decimal,
    # String types.
    1042: DataType.String,  # bpchar
    25: DataType.String,  # text
    1043: DataType.String,  # varchar
    # Timestamp types.
    1114: DataType.Timestamp,  # timestamp
    1083: DataType.Timestamp,  # time
    # N.B. We do not currently support date types.
}
