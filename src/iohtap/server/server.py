import pyodbc

from iohtap.config.dbtype import DBType
from iohtap.config.file import Config


class IOHTAPServer:
    def __init__(self, host: str, port: int, config_file: str):
        self._host = host
        self._port = port
        self._config = Config(config_file)

    def run_test(self):
        # A temporary method for testing purposes.
        cstr = self._config.get_odbc_connection_string(DBType.Aurora)
        conn = pyodbc.connect(cstr)
        conn.setencoding("UTF-8")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM inventory LIMIT 1")
        row = cursor.fetchone()
        if row:
            print(row)
