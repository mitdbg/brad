import pyodbc

from brad.grpc_client import BradGrpcClient, RowList


class Database:
    def execute_sync(self, query: str) -> RowList:
        raise NotImplementedError

    def commit_sync(self) -> None:
        raise NotImplementedError

    def rollback_sync(self) -> None:
        raise NotImplementedError

    def close_sync(self) -> None:
        raise NotImplementedError


class PyodbcDatabase(Database):
    def __init__(self, connection) -> None:
        self._conn = connection
        self._cursor = self._conn.cursor()

    def execute_sync(self, query: str) -> RowList:
        self._cursor.execute(query)
        try:
            rows = self._cursor.fetchall()
            return list(rows)
        except pyodbc.ProgrammingError:
            return []

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

    def commit_sync(self) -> None:
        self._brad.run_query_ignore_results("COMMIT")

    def rollback_sync(self) -> None:
        self._brad.run_query_ignore_results("ROLLBACK")

    def close_sync(self) -> None:
        self._brad.close()
