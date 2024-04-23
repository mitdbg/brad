import logging
import sqlite3
import datetime
import decimal
from typing import Any, Optional, List, Iterable

from brad.connection.cursor import Cursor, Row
from brad.connection.schema import Schema, Field, DataType

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

    def result_schema(self, results: Optional[List[Row]] = None) -> Schema:
        assert results is not None
        fields = []
        for idx, column_metadata in enumerate(self._cursor_impl.description):
            column_name = column_metadata[0]
            brad_type = _deduce_type(results, idx)
            fields.append(Field(name=column_name, data_type=brad_type))
        return Schema(fields)

    def commit_sync(self) -> None:
        self._conn_impl.commit()

    def rollback_sync(self) -> None:
        self._conn_impl.rollback()


def _deduce_type(results: List[Row], column_index: int) -> DataType:
    # SQLite is not strictly-typed (https://www.sqlite.org/flextypegood.html).
    # Therefore we cannot retrieve type information from the database cursor.
    # Instead, we manually deduce the type by running the type tests below and
    # assume that all values in the same column have the same type.
    #
    # This is an acceptable workaround because our SQLite "back end" is only
    # used for development purposes (to run the rest of BRAD without having to
    # start engines on AWS).

    for row in results:
        value = row[column_index]
        if value is None:
            continue

        value_type = type(value)
        if value_type is int:
            return DataType.Integer
        elif value_type is str:
            return DataType.String
        elif value_type is float:
            return DataType.Float
        elif value_type is decimal.Decimal:
            return DataType.Decimal
        elif value_type is bool:
            return DataType.Integer
        elif value_type is datetime.datetime:
            return DataType.Timestamp
        else:
            return DataType.Unknown

    return DataType.Unknown
