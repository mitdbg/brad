import pyodbc
import mysql.connector
import psycopg2
import sys

from typing import Tuple, Optional
from brad.config.engine import Engine
from brad.grpc_client import BradGrpcClient, RowList, BradClientError
from brad.connection.connection import Connection


class Database:
    def execute_sync(self, query: str) -> RowList:
        raise NotImplementedError

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        raise NotImplementedError

    def begin_sync(self) -> None:
        raise NotImplementedError

    def commit_sync(self) -> None:
        raise NotImplementedError

    def rollback_sync(self) -> None:
        raise NotImplementedError

    def close_sync(self) -> None:
        raise NotImplementedError


class PyodbcDatabase(Database):
    def __init__(self, connection, engine: Optional[Engine] = None) -> None:
        self._conn = connection
        self._engine = engine
        self._cursor = None

    def should_fetch(self, query, cur) -> bool:
        is_select = query.strip().lower().startswith("select")
        has_rows = cur.rowcount is not None and cur.rowcount > 0
        if self._engine == Engine.Aurora or self._engine == Engine.Redshift:
            return is_select
        else:
            return is_select or has_rows

    def execute_sync(self, query: str) -> RowList:
        # print(f"Running Query: {query}")
        try:
            # Get cursor
            if self._cursor is None:
                had_cursor = False
                cursor = self._conn.cursor()
            else:
                had_cursor = True
                cursor = self._cursor
            # Exec
            cursor.execute(query)
            if self.should_fetch(query, cursor):
                rows = cursor.fetchall()
            else:
                rows = []
            # Close if newly opened.
            if not had_cursor:
                cursor.close()
            # Return
            return list(rows)
        except pyodbc.ProgrammingError:
            return []
        except mysql.connector.errors.DatabaseError as e:
            msg = f"Transient error: {e}.\nQuery: {query}"
            raise BradClientError(msg, is_transient=True)
        except mysql.connector.errors.InterfaceError as e:
            msg = f"Transient error: {e}.\nQuery: {query}"
            raise BradClientError(msg, is_transient=True)
        except psycopg2.errors.DatabaseError as e:
            msg = f"Transient error: {e}.\nQuery: {query}"
            raise BradClientError(msg, is_transient=True)
        except psycopg2.errors.InterfaceError as e:
            # Restart the connection.
            msg = f"Transient error: {e}.\nQuery: {query}"
            raise BradClientError(msg, is_transient=True)
        

    def begin_sync(self) -> None:
        # Open a new cursor
        self._cursor = self._conn.cursor()

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        return self.execute_sync(query), self._engine

    def commit_sync(self) -> None:
        if self._cursor is not None:
            self._cursor.execute("COMMIT")
        self._cursor = None

    def rollback_sync(self) -> None:
        if self._cursor is not None:
            self._cursor.execute("ROLLBACK")
        self._cursor = None

    def close_sync(self) -> None:
        if self._cursor is not None:
            self._cursor.close()
        self._conn.close()


class BradDatabase(Database):
    def __init__(self, brad_client: BradGrpcClient) -> None:
        self._brad = brad_client

    def begin_sync(self) -> None:
        self._brad.run_query_ignore_results("BEGIN")

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
    def __init__(self, connection: Connection) -> None:
        self._conn = connection
        self._cursor = connection.cursor_sync()

    def execute_sync(self, query: str) -> RowList:
        self._cursor.execute_sync(query)
        try:
            return self._cursor.fetchall_sync()
        except pyodbc.ProgrammingError:
            # Happens when we call `fetchall()` after running a DML statement.
            return []

    def execute_sync_with_engine(self, query: str) -> Tuple[RowList, Optional[Engine]]:
        return self.execute_sync(query), None

    def begin_sync(self) -> None:
        self._cursor.execute_sync("BEGIN")

    def commit_sync(self) -> None:
        self._cursor.commit_sync()

    def rollback_sync(self) -> None:
        self._cursor.rollback_sync()

    def close_sync(self) -> None:
        self._conn.close_sync()
