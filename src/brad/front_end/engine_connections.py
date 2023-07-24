import asyncio
import logging
from typing import Optional, Dict, Set

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory

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
        specific_engines: Optional[Set[Engine]] = None,
    ) -> "EngineConnections":
        """
        Establishes connections to the underlying engines.
        """

        # As the system gets more sophisticated, we'll add connection pooling, etc.
        logger.debug(
            "Establishing a new set of connections to the underlying engines..."
        )

        if specific_engines is None:
            specific_engines = {Engine.Aurora, Engine.Redshift, Engine.Athena}

        connection_map: Dict[Engine, Connection] = {}

        for engine in specific_engines:
            logger.debug("Connecting to %s...", engine)
            connection_map[engine] = await ConnectionFactory.connect_to(
                engine, schema_name, config, autocommit
            )

            # TODO: We may want this to be configurable.
            if engine == Engine.Redshift:
                conn = connection_map[engine]
                cursor = await conn.cursor()
                await cursor.execute("SET enable_result_cache_for_session = off")

        return cls(connection_map, schema_name)

    @classmethod
    def connect_sync(
        cls,
        config: ConfigFile,
        schema_name: Optional[str] = None,
        autocommit: bool = True,
        specific_engines: Optional[Set[Engine]] = None,
    ) -> "EngineConnections":
        """
        Synchronously establishes connections to the underlying engines. The
        connections made by this method are `pyodbc` connections.
        """

        # As the system gets more sophisticated, we'll add connection pooling, etc.
        logger.debug(
            "Establishing a new set of connections to the underlying engines..."
        )

        if specific_engines is None:
            specific_engines = {Engine.Aurora, Engine.Redshift, Engine.Athena}

        connection_map: Dict[Engine, Connection] = {}

        for engine in specific_engines:
            logger.debug("Connecting to %s...", engine)
            connection_map[engine] = ConnectionFactory.connect_to_sync(
                engine, schema_name, config, autocommit
            )

            # TODO: We may want this to be configurable.
            if engine == Engine.Redshift:
                conn = connection_map[engine]
                cursor = conn.cursor_sync()
                cursor.execute_sync("SET enable_result_cache_for_session = off")

        return cls(connection_map, schema_name)

    def __init__(
        self, connection_map: Dict[Engine, Connection], schema_name: Optional[str]
    ):
        # NOTE: Need to set the appropriate isolation levels.
        self._connection_map = connection_map
        self._schema_name = schema_name

    def __del__(self) -> None:
        self.close_sync()

    @property
    def schema_name(self) -> Optional[str]:
        return self._schema_name

    def get_connection(self, engine: Engine) -> Connection:
        try:
            return self._connection_map[engine]
        except KeyError as ex:
            raise RuntimeError("Not connected to {}".format(engine)) from ex

    async def close(self):
        """
        Close the underlying connections. This instance can no longer be used after
        calling this method.
        """
        futures = []
        for conn in self._connection_map.values():
            futures.append(conn.close())
        await asyncio.gather(*futures)
        self._connection_map.clear()

    def close_sync(self):
        """
        Close the underlying connections. This instance can no longer be used after
        calling this method.
        """
        for conn in self._connection_map.values():
            conn.close_sync()
        self._connection_map.clear()
