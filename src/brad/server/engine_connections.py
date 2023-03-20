import logging
import pyodbc

from brad.config.dbtype import DBType
from brad.config.file import ConfigFile

logger = logging.getLogger(__name__)


class EngineConnections:
    """
    Manages connections to the underlying database systems.
    """

    @classmethod
    def connect(
        cls, config: ConfigFile, autocommit: bool = True
    ) -> "EngineConnections":
        # As the system gets more sophisticated, we'll add connection pooling, etc.
        logger.info("Establishing connections to the underlying database systems...")
        logger.debug("Connecting to Athena...")
        athena = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Athena), autocommit=autocommit
        )
        logger.debug("Connecting to Aurora...")
        aurora = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Aurora), autocommit=autocommit
        )
        logger.debug("Connecting to Redshift...")
        redshift = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Redshift), autocommit=autocommit
        )
        redshift.execute("SET enable_result_cache_for_session = off")
        return cls(athena, aurora, redshift)

    def __init__(self, athena, aurora, redshift):
        # NOTE: Need to set the appropriate isolation levels.
        self._athena = athena
        self._aurora = aurora
        self._redshift = redshift

    def get_connection(self, db: DBType):
        if db == DBType.Athena:
            return self._athena
        elif db == DBType.Aurora:
            return self._aurora
        elif db == DBType.Redshift:
            return self._redshift
        else:
            raise AssertionError("Unsupported database type: " + str(db))
