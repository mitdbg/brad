import pyodbc
from typing import Generator, Tuple, List, Any


class BradFlightSqlClientOdbc:
    """
    A client that communicates with BRAD via Arrow Flight SQL ODBC driver.

    Usage:
    ```
    with BradFlightSqlClient(host, port) as client:
        for row in client.run_query(session_id, "SELECT 1"):
            print(row)
    ```
    """

    RowList = List[Tuple[Any, ...]]

    def __init__(self, host="localhost", port=31337) -> None:
        self._host = host
        self._port = port
        self._connection = None
        self._cursor = None

    def __enter__(self) -> None:
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
        self._cursor.close()
        self._connection.close()

    def run_query_generator(self, query: str) -> Generator[Tuple[Any, ...], None, None]:
        for row in self._cursor.execute(query):
            yield row

    def run_query(self, query: str) -> RowList:
        return self._cursor.execute(query)
