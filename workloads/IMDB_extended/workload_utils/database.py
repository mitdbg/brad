import pyodbc
import sys

from typing import Tuple, Optional, Callable
from brad.config.engine import Engine
from brad.grpc_client import BradGrpcClient, RowList
from brad.connection.connection import Connection


class Database:
    def execute_sync(self, query: str) -> RowList:
        raise NotImplementedError

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        raise NotImplementedError

    def commit_sync(self) -> None:
        raise NotImplementedError

    def rollback_sync(self) -> None:
        raise NotImplementedError

    def close_sync(self) -> None:
        raise NotImplementedError

    def reconnect(self) -> None:
        print("Reconnecting not supported.", file=sys.stderr, flush=True)


class PyodbcDatabase(Database):
    def __init__(self, connection, engine: Optional[Engine] = None) -> None:
        self._conn = connection
        self._cursor = self._conn.cursor()
        self._engine = engine

    def execute_sync(self, query: str) -> RowList:
        self._cursor.execute(query)
        try:
            rows = self._cursor.fetchall()
            return list(rows)
        except pyodbc.ProgrammingError:
            return []

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        return self.execute_sync(query), self._engine

    def commit_sync(self) -> None:
        self._cursor.execute("COMMIT")

    def rollback_sync(self) -> None:
        self._cursor.execute("ROLLBACK")

    def close_sync(self) -> None:
        self._conn.close()


class BradDatabase(Database):
    def __init__(self, brad_client: BradGrpcClient) -> None:
        self._brad = brad_client

    def execute_sync(self, query: str) -> RowList:
        rows, _ = self._brad.run_query_json(query)
        return rows

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        return self._brad.run_query_json(query)

    def commit_sync(self) -> None:
        self._brad.run_query_ignore_results("COMMIT")

    def rollback_sync(self) -> None:
        self._brad.run_query_ignore_results("ROLLBACK")

    def close_sync(self) -> None:
        self._brad.close()


class DirectConnection(Database):
    def __init__(
        self, connection: Connection, do_reconnect: Callable[[], Connection]
    ) -> None:
        self._conn = connection
        self._cursor = connection.cursor_sync()
        self._do_reconnect = do_reconnect

    def execute_sync(self, query: str) -> RowList:
        self._cursor.execute_sync(query)
        try:
            return self._cursor.fetchall_sync()
        except pyodbc.ProgrammingError:
            # Happens when we call `fetchall()` after running a DML statement.
            return []

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        return self.execute_sync(query), None

    def commit_sync(self) -> None:
        self._cursor.commit_sync()

    def rollback_sync(self) -> None:
        self._cursor.rollback_sync()

    def close_sync(self) -> None:
        self._conn.close_sync()

    def reconnect(self) -> None:
        try:
            self._conn = self._do_reconnect()
            self._cursor = self._conn.cursor_sync()
        except Exception as ex:
            print(
                "Direct connection reconnect failed.",
                str(ex),
                file=sys.stderr,
                flush=True,
            )
