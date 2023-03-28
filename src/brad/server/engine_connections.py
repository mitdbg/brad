import logging
import aioodbc
import pyodbc
from typing import Optional

from brad.config.dbtype import DBType
from brad.config.file import ConfigFile

logger = logging.getLogger(__name__)


class EngineConnections:
    """
    Manages connections to the underlying database systems.
    """

    @classmethod
    async def connect(
        cls,
        config: ConfigFile,
        database_name: Optional[str] = None,
        autocommit: bool = True,
    ) -> "EngineConnections":
        """
        Establishes connections to the underlying engines. The connections made
        by this method are `aioodbc` connections.
        """

        # As the system gets more sophisticated, we'll add connection pooling, etc.
        logger.debug(
            "Establishing a new set of connections to the underlying engines..."
        )
        logger.debug("Connecting to Athena...")
        athena = await aioodbc.connect(
            dsn=config.get_odbc_connection_string(DBType.Athena, database_name),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Aurora...")
        aurora = await aioodbc.connect(
            dsn=config.get_odbc_connection_string(DBType.Aurora, database_name),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Redshift...")
        redshift = await aioodbc.connect(
            dsn=config.get_odbc_connection_string(DBType.Redshift, database_name),
            autocommit=autocommit,
        )
        await redshift.execute("SET enable_result_cache_for_session = off")
        return cls(athena, aurora, redshift, database_name)

    @classmethod
    def connect_sync(
        cls,
        config: ConfigFile,
        database_name: Optional[str] = None,
        autocommit: bool = True,
    ) -> "EngineConnections":
        """
        Synchronously establishes connections to the underlying engines. The
        connections made by this method are `pyodbc` connections.
        """

        # As the system gets more sophisticated, we'll add connection pooling, etc.
        logger.debug(
            "Establishing a new set of connections to the underlying engines..."
        )
        logger.debug("Connecting to Athena...")
        athena = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Athena, database_name),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Aurora...")
        aurora = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Aurora, database_name),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Redshift...")
        redshift = pyodbc.connect(
            config.get_odbc_connection_string(DBType.Redshift, database_name),
            autocommit=autocommit,
        )
        redshift.execute("SET enable_result_cache_for_session = off")
        return cls(athena, aurora, redshift, database_name)

    def __init__(self, athena, aurora, redshift, database_name: Optional[str]):
        # NOTE: Need to set the appropriate isolation levels.
        self._athena = athena
        self._aurora = aurora
        self._redshift = redshift
        self._database_name = database_name

    @property
    def database_name(self) -> Optional[str]:
        return self._database_name

    def get_connection(self, db: DBType):
        if db == DBType.Athena:
            return self._athena
        elif db == DBType.Aurora:
            return self._aurora
        elif db == DBType.Redshift:
            return self._redshift
        else:
            raise AssertionError("Unsupported database type: " + str(db))

    async def close(self):
        """
        Call to close the connections when opened in async mode.
        """
        await self._athena.close()
        await self._aurora.close()
        await self._redshift.close()
