import pyodbc

from iohtap.config.dbtype import DBType
from iohtap.config.file import ConfigFile


class DBConnectionManager:
    """
    Manages connections to the underlying database systems.
    """

    def __init__(self, config: ConfigFile):
        self._config = config
        # To start, we just hold one set of connections. As things get more
        # sophisticated, we'll add connection pooling, etc.
        self._athena = pyodbc.connect(config.get_odbc_connection_string(DBType.Athena))
        self._aurora = pyodbc.connect(config.get_odbc_connection_string(DBType.Aurora))
        self._redshift = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Redshift)
        )

    def get_connection(self, db: DBType):
        if db == DBType.Athena:
            return self._athena
        elif db == DBType.Aurora:
            return self._aurora
        elif db == DBType.Redshift:
            return self._redshift
        else:
            raise AssertionError("Unsupported database type: " + str(db))
