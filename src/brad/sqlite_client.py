import sqlite3
from typing import Generator, Tuple, List, Any


class BradSqliteClient:
    """
    A client that communicates with BRAD directly against SQLite database.

    Usage:
    ```
    with BradSqlClient(host, port) as client:
        for row in client.run_query(session_id, "SELECT 1"):
            print(row)
    ```
    """

    RowList = List[Tuple[Any, ...]]

    def __init__(self, database: str) -> None:
        self._database = database
        self._connection = None
        self._cursor = None

    def __enter__(self) -> None:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def connect(self) -> None:
        self._connection = sqlite3.connect(self._database)
        self._cursor = self._connection.cursor()

    def close(self) -> None:
        self._cursor.close()
        self._connection.close()

    def run_query_generator(self, query: str) -> Generator[Tuple[Any, ...], None, None]:
        assert self._cursor
        for row in self._cursor.execute(query):
            yield row

    def run_query(self, query: str) -> RowList:
        assert self._cursor
        return self._cursor.execute(query)
