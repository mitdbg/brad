import sqlite3
from typing import Generator, Optional, Self, Tuple, Any


class BradSqliteClient:
    """
    A client that communicates with BRAD directly against SQLite database.

    Usage:
    ```
    with BradSqliteClient(database) as client:
        for row in client.run_query(session_id, "SELECT 1"):
            print(row)
    ```
    """

    def __init__(self, database: str) -> None:
        self._database = database
        self._connection: Optional[sqlite3.Connection] = None
        self._cursor: Optional[sqlite3.Cursor] = None

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def connect(self) -> None:
        self._connection = sqlite3.connect(self._database)
        self._cursor = self._connection.cursor()

    def close(self) -> None:
        assert self._cursor
        assert self._connection
        self._cursor.close()
        self._connection.close()

    def run_query_generator(self, query: str) -> Generator[Tuple[Any, ...], None, None]:
        assert self._cursor
        for row in self._cursor.execute(query):
            yield row

    def run_query(self, query: str) -> sqlite3.Cursor:
        assert self._cursor
        return self._cursor.execute(query)
