import asyncio
import redshift_connector
from redshift_connector.utils.oids import RedshiftOID
from typing import Any, Iterable, Optional, List

from .cursor import Cursor, Row
from .schema import Schema, Field, DataType


class RedshiftCursor(Cursor):
    def __init__(
        self, impl: redshift_connector.Cursor, conn: redshift_connector.Connection
    ) -> None:
        super().__init__()
        self._impl = impl
        self._conn = conn

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
        raise RuntimeError("Not supported on Redshift.")

    def fetchone_sync(self) -> Optional[Row]:
        return self._impl.fetchone()

    def fetchall_sync(self) -> List[Row]:
        res = self._impl.fetchall()
        return res

    def result_schema(self, results: Optional[List[Row]] = None) -> Schema:
        fields = []
        for column_metadata in self._impl.description:
            column_name = column_metadata[0]
            redshift_oid = column_metadata[1]
            try:
                brad_type = _REDSHIFT_OID_TO_BRAD_TYPE[redshift_oid]
            except KeyError:
                brad_type = DataType.Unknown
            fields.append(Field(name=column_name, data_type=brad_type))
        return Schema(fields)

    def commit_sync(self) -> None:
        self._conn.commit()

    def rollback_sync(self) -> None:
        self._conn.rollback()


_REDSHIFT_OID_TO_BRAD_TYPE = {
    # Integer types.
    RedshiftOID.INTEGER: DataType.Integer,
    RedshiftOID.BIGINT: DataType.Integer,
    RedshiftOID.BOOLEAN: DataType.Integer,
    RedshiftOID.SMALLINT: DataType.Integer,
    RedshiftOID.OID: DataType.Integer,
    RedshiftOID.ROWID: DataType.Integer,
    # Float types.
    RedshiftOID.FLOAT: DataType.Float,
    # Fixed precision types.
    RedshiftOID.NUMERIC: DataType.Decimal,
    RedshiftOID.DECIMAL: DataType.Decimal,
    # String types.
    RedshiftOID.CHAR: DataType.String,
    RedshiftOID.CSTRING: DataType.String,
    RedshiftOID.STRING: DataType.String,
    RedshiftOID.TEXT: DataType.String,
    RedshiftOID.VARCHAR: DataType.String,
    RedshiftOID.BPCHAR: DataType.String,
    # Timestamp types.
    RedshiftOID.TIMESTAMP: DataType.Timestamp,
    RedshiftOID.TIME: DataType.Timestamp,
    # N.B. We do not currently support date types.
}
