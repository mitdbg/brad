import pyodbc

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile


class IOHTAPServer:
    def __init__(self, config: ConfigFile):
        self._config = config

    def run_test(self):
        # A temporary method for testing purposes.
        cstr = self._config.get_odbc_connection_string(DBType.Athena)
        conn = pyodbc.connect(cstr)
        conn.setencoding("UTF-8")
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        row = cursor.fetchone()
        if row:
            print(row)
