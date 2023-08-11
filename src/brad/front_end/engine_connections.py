import asyncio
import logging
from typing import Optional, Dict, Set

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection, ConnectionFailed
from brad.connection.factory import ConnectionFactory
from brad.provisioning.directory import Directory

logger = logging.getLogger(__name__)


class EngineConnections:
    """
    Manages connections to the underlying database systems.
    """

    @classmethod
    async def connect(
        cls,
        config: ConfigFile,
        directory: Directory,
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
                engine, schema_name, config, directory, autocommit
            )

            # TODO: We may want this to be configurable.
            if engine == Engine.Redshift:
                conn = connection_map[engine]
                cursor = await conn.cursor()
                await cursor.execute("SET enable_result_cache_for_session = off")

        return cls(connection_map, schema_name, autocommit)

    @classmethod
    def connect_sync(
        cls,
        config: ConfigFile,
        directory: Directory,
        schema_name: Optional[str] = None,
        autocommit: bool = True,
        specific_engines: Optional[Set[Engine]] = None,
    ) -> "EngineConnections":
        """
        Synchronously establishes connections to the underlying engines.
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
                engine, schema_name, config, directory, autocommit
            )

            # TODO: We may want this to be configurable.
            if engine == Engine.Redshift:
                conn = connection_map[engine]
                cursor = conn.cursor_sync()
                cursor.execute_sync("SET enable_result_cache_for_session = off")

        return cls(connection_map, schema_name, autocommit)

    def __init__(
        self,
        connection_map: Dict[Engine, Connection],
        schema_name: Optional[str],
        autocommit: bool,
    ):
        # NOTE: Need to set the appropriate isolation levels.
        self._connection_map = connection_map
        self._schema_name = schema_name
        self._autocommit = autocommit

    def __del__(self) -> None:
        self.close_sync()

    @property
    def schema_name(self) -> Optional[str]:
        return self._schema_name

    async def add_connections(
        self, config: ConfigFile, directory: Directory, expected_engines: Set[Engine]
    ) -> None:
        """
        Adds connections to engines that are in `expected_engines` but not
        currently connected to.
        """
        for engine in expected_engines:
            if engine in self._connection_map:
                continue
            self._connection_map[engine] = await ConnectionFactory.connect_to(
                engine, self._schema_name, config, directory, self._autocommit
            )

            # TODO: We may want this to be configurable.
            if engine == Engine.Redshift:
                cursor = self._connection_map[engine].cursor_sync()
                cursor.execute_sync("SET enable_result_cache_for_session = off")

    async def remove_connections(self, expected_engines: Set[Engine]) -> None:
        """
        Removes connections from engines that are not in `expected_engines` but
        are currently connected to.
        """
        to_remove = []
        for engine, conn in self._connection_map.items():
            if engine in expected_engines:
                continue
            await conn.close()
            to_remove.append(engine)

        for engine in to_remove:
            del self._connection_map[engine]

    async def reestablish_connections(
        self, config: ConfigFile, directory: Directory
    ) -> bool:
        """
        Used to reconnect to engines when a connection has been lost. Lost
        connections may occur during blueprint transitions when the provisioning
        changes (e.g., an Aurora failover). This method returns `True` iff all
        of the lost connections were re-established.

        Callers should take care to not call this method repeatedly to avoid
        overwhelming the underlying engines. Use randomized exponential backoff
        instead.
        """
        new_connections = []
        all_succeeded = True

        for engine, conn in self._connection_map.items():
            if conn.is_connected():
                continue
            try:
                new_conn = await ConnectionFactory.connect_to(
                    engine, self._schema_name, config, directory, self._autocommit
                )
                # TODO: We may want this to be configurable.
                if engine == Engine.Redshift:
                    cursor = new_conn.cursor_sync()
                    cursor.execute_sync("SET enable_result_cache_for_session = off")
                new_connections.append((engine, new_conn))
            except ConnectionFailed:
                all_succeeded = False

        for engine, conn in new_connections:
            self._connection_map[engine] = conn

        return all_succeeded

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
