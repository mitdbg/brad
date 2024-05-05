import pyodbc
from typing import Generator, Tuple, Any

class BradFlightSqlClient:
    """
    A client that communicates with BRAD via Arrow Flight SQL ODBC driver.

    Usage:
    ```
    with BradFlightSqlClient(host, port) as client:
        for row in client.run_query(session_id, "SELECT 1"):
            print(row)
    ```
    """

    def __init__(self, host="localhost", port=31337):
        self._host = host
        self._port = port

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def connect(self):
        self._connection = pyodbc.connect(
            "DRIVER={Arrow Flight SQL ODBC Driver};USEENCRYPTION=false;" + 
            f"HOST={self._host};" +
            f"PORT={self._port}",
            autocommit=True)
        self._cursor = self._connection.cursor()

    def close(self):
        self._cursor.close()
        self._connection.close()

    def run_query(self, query: str) -> Generator[Tuple[Any, ...], None, None]:
        for row in self._cursor.execute(query):
            yield row

if __name__ == "__main__":
    with BradFlightSqlClient() as client:
        for row in client.run_query("SELECT 1"):
            print(row)