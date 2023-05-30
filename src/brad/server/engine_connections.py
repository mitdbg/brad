import logging
import aioodbc
import pyodbc
from typing import Optional, Dict, Any

from brad.config.engine import Engine
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
        schema_name: Optional[str] = None,
        autocommit: bool = True,
        read_only: bool = False,
        conn_info: Optional[Dict[Engine, Any]] = None,
    ) -> "EngineConnections":
        """
        Establishes connections to the underlying engines. The connections made
        by this method are `aioodbc` connections.
        """
        if conn_info is None:
            conn_info = {}  # Mark all as missing.

        # As the system gets more sophisticated, we'll add connection pooling, etc.
        logger.debug(
            "Establishing a new set of connections to the underlying engines..."
        )
        logger.debug("Connecting to Athena...")
        athena = await aioodbc.connect(
            dsn=config.get_odbc_connection_string(
                Engine.Athena, schema_name, conn_info.get(Engine.Athena)
            ),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Aurora...")
        aurora = await aioodbc.connect(
            dsn=config.get_odbc_connection_string(
                Engine.Aurora, schema_name, (read_only, conn_info.get(Engine.Aurora))
            ),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Redshift...")
        redshift = await aioodbc.connect(
            dsn=config.get_odbc_connection_string(
                Engine.Redshift, schema_name, conn_info.get(Engine.Redshift)
            ),
            autocommit=autocommit,
        )
        await redshift.execute("SET enable_result_cache_for_session = off")
        return cls(athena, aurora, redshift, schema_name)

    @classmethod
    def connect_sync(
        cls,
        config: ConfigFile,
        schema_name: Optional[str] = None,
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
            config.get_odbc_connection_string(Engine.Athena, schema_name),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Aurora...")
        aurora = pyodbc.connect(
            config.get_odbc_connection_string(Engine.Aurora, schema_name),
            autocommit=autocommit,
        )
        logger.debug("Connecting to Redshift...")
        redshift = pyodbc.connect(
            config.get_odbc_connection_string(Engine.Redshift, schema_name),
            autocommit=autocommit,
        )
        redshift.execute("SET enable_result_cache_for_session = off")
        return cls(athena, aurora, redshift, schema_name)

    def __init__(self, athena, aurora, redshift, schema_name: Optional[str]):
        # NOTE: Need to set the appropriate isolation levels.
        self._athena = athena
        self._aurora = aurora
        self._redshift = redshift
        self._schema_name = schema_name

    @property
    def schema_name(self) -> Optional[str]:
        return self._schema_name

    def get_connection(self, db: Engine):
        if db == Engine.Athena:
            return self._athena
        elif db == Engine.Aurora:
            return self._aurora
        elif db == Engine.Redshift:
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

    def close_sync(self):
        self._athena.close()
        self._aurora.close()
        self._redshift.close()
