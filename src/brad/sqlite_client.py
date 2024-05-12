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


    def __init__(self, database: str):
        self._database = database

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        self._connection = sqlite3.connect(self._database)
        self._cursor = self._connection.cursor()

    def close(self):
        self._cursor.close()
        self._connection.close()

    def run_query_generator(self, query: str) -> Generator[Tuple[Any, ...], None, None]:
        for row in self._cursor.execute(query):
            yield row

    def run_query(self, query: str) -> RowList:
        return self._cursor.execute(query)

if __name__ == "__main__":
     with BradSqliteClient(database="/tmp/sophiez_brad_stub_db.sqlite") as client:
        for row in client.run_query("SELECT 1"):
            print(row)