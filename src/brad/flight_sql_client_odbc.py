import pyodbc
from typing import Generator, Optional, Self, Tuple, List, Any


class BradFlightSqlClientOdbc:
    """
    A client that communicates with BRAD via Arrow Flight SQL ODBC driver.

    Usage:
    ```
    with BradFlightSqlClientOdbc(host, port) as client:
        for row in client.run_query("SELECT 1"):
            print(row)
    ```
    """

    RowList = List[Tuple[Any, ...]]

    def __init__(self, host="localhost", port=31337) -> None:
        self._host = host
        self._port = port
        self._connection: Optional[pyodbc.Connection] = None
        self._cursor: Optional[pyodbc.Cursor] = None

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def connect(self) -> None:
        self._connection = pyodbc.connect(
            "DRIVER={Arrow Flight SQL ODBC Driver};USEENCRYPTION=false;"
            + f"HOST={self._host};"
            + f"PORT={self._port}",
            autocommit=True,
        )
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

    def run_query(self, query: str) -> RowList:
        assert self._cursor
        return self._cursor.execute(query)
