import asyncio
import logging
import random
from typing import Optional, Dict, Set, List

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
        connect_to_aurora_read_replicas: bool = False,
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
        aurora_read_replicas: List[Connection] = []

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

            if engine == Engine.Aurora and connect_to_aurora_read_replicas:
                # NOTE: We want to avoid using the reader endpoint so that we
                # have more control over load balancing.
                aurora_read_replicas = await cls._connect_to_aurora_replicas(
                    directory, schema_name, config, autocommit
                )

        return cls(
            connection_map,
            schema_name,
            autocommit,
            connect_to_aurora_read_replicas,
            aurora_read_replicas,
        )

    @classmethod
    def connect_sync(
        cls,
        config: ConfigFile,
        directory: Directory,
        schema_name: Optional[str] = None,
        autocommit: bool = True,
        specific_engines: Optional[Set[Engine]] = None,
        connect_to_aurora_read_replicas: bool = False,
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
        aurora_read_replicas: List[Connection] = []

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

            if engine == Engine.Aurora and connect_to_aurora_read_replicas:
                aurora_read_replicas = cls._connect_to_aurora_replicas_sync(
                    directory, schema_name, config, autocommit
                )

        return cls(
            connection_map,
            schema_name,
            autocommit,
            connect_to_aurora_read_replicas,
            aurora_read_replicas,
        )

    def __init__(
        self,
        connection_map: Dict[Engine, Connection],
        schema_name: Optional[str],
        autocommit: bool,
        connect_to_aurora_read_replicas: bool,
        aurora_read_replicas: List[Connection],
    ):
        self._connection_map = connection_map
        self._schema_name = schema_name
        self._autocommit = autocommit
        self._closed = False
        self._connect_to_aurora_read_replicas = connect_to_aurora_read_replicas
        self._aurora_read_replicas = aurora_read_replicas
        self._prng = random.Random()

    def __del__(self) -> None:
        self.close_sync()

    @property
    def schema_name(self) -> Optional[str]:
        return self._schema_name

    async def add_and_refresh_connections(
        self, config: ConfigFile, directory: Directory, expected_engines: Set[Engine]
    ) -> None:
        """
        Adds connections to engines that are in `expected_engines` but not
        currently connected to. This will also reconnect to Redshift because we
        may change the underlying physical endpoint.
        """
        for engine in expected_engines:
            if engine in self._connection_map and engine != Engine.Redshift:
                continue
            # We force reconnect to Redshift because we may be changing to a
            # different physical endpoint.
            self._connection_map[engine] = await ConnectionFactory.connect_to(
                engine, self._schema_name, config, directory, self._autocommit
            )

            # TODO: We may want this to be configurable.
            if engine == Engine.Redshift:
                cursor = self._connection_map[engine].cursor_sync()
                cursor.execute_sync("SET enable_result_cache_for_session = off")

        if self._connect_to_aurora_read_replicas and Engine.Aurora in expected_engines:
            # For simplicity, refresh all connections. This is because of how we
            # update the replica set (sometimes we create an entirely new
            # replica to replace an existing one).
            #
            # N.B. There may be existing clients using the current connections.
            # For simplicity, just replace the connections list.
            self._aurora_read_replicas = await self._connect_to_aurora_replicas(
                directory, self._schema_name, config, self._autocommit
            )

    async def remove_connections(
        self, expected_engines: Set[Engine], expected_aurora_read_replicas: int
    ) -> None:
        """
        Removes connections from engines that are not in `expected_engines` but
        are currently connected to.
        """
        to_remove = []
        curr_connections = [item for item in self._connection_map.items()]
        for engine, conn in curr_connections:
            if engine in expected_engines:
                continue
            await conn.close()
            to_remove.append(engine)

        for engine in to_remove:
            del self._connection_map[engine]

        all_replicas_to_remove = []
        while len(self._aurora_read_replicas) > expected_aurora_read_replicas:
            all_replicas_to_remove.append(self._aurora_read_replicas.pop())

        for replica_to_remove in all_replicas_to_remove:
            await replica_to_remove.close()

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
        curr_connections = [item for item in self._connection_map.items()]
        new_connections = []
        all_succeeded = True

        for engine, conn in curr_connections:
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

        # Reconnect to read replicas if needed.
        if (
            self._connect_to_aurora_read_replicas
            and Engine.Aurora in self._connection_map
        ):
            curr_replica_conns = self._aurora_read_replicas.copy()
            new_replica_conns = []
            for replica_idx, conn in enumerate(curr_replica_conns):
                if conn.is_connected():
                    continue
                try:
                    new_conn = await ConnectionFactory.connect_to(
                        Engine.Aurora,
                        self._schema_name,
                        config,
                        directory,
                        self._autocommit,
                        replica_idx,
                    )
                    new_replica_conns.append((replica_idx, new_conn))
                except ConnectionFailed:
                    all_succeeded = False

            for replica_idx, conn in new_replica_conns:
                self._aurora_read_replicas[replica_idx] = conn

        return all_succeeded

    def get_connection(self, engine: Engine) -> Connection:
        try:
            return self._connection_map[engine]
        except KeyError as ex:
            raise RuntimeError("Not connected to {}".format(engine)) from ex

    def get_reader_connection(
        self, engine: Engine, specific_index: Optional[int] = None
    ) -> Connection:
        if engine != Engine.Aurora or len(self._aurora_read_replicas) == 0:
            return self.get_connection(engine)

        if specific_index is not None:
            return self._aurora_read_replicas[specific_index]
        else:
            return self._prng.choice(self._aurora_read_replicas)

    async def close(self):
        """
        Close the underlying connections. This instance can no longer be used after
        calling this method.
        """
        if self._closed:
            return

        futures = []
        for conn in self._connection_map.values():
            futures.append(conn.close())
        await asyncio.gather(*futures)
        self._closed = True

    def close_sync(self):
        """
        Close the underlying connections. This instance can no longer be used after
        calling this method.
        """
        if self._closed:
            return

        for conn in self._connection_map.values():
            conn.close_sync()
        self._closed = True

    @staticmethod
    async def _connect_to_aurora_replicas(
        directory: Directory,
        schema_name: Optional[str],
        config: ConfigFile,
        autocommit: bool,
    ) -> List[Connection]:
        aurora_read_replicas = []
        for replica_index in range(len(directory.aurora_readers())):
            aurora_read_replicas.append(
                await ConnectionFactory.connect_to(
                    Engine.Aurora,
                    schema_name,
                    config,
                    directory,
                    autocommit,
                    replica_index,
                )
            )
        return aurora_read_replicas

    @staticmethod
    def _connect_to_aurora_replicas_sync(
        directory: Directory,
        schema_name: Optional[str],
        config: ConfigFile,
        autocommit: bool,
    ) -> List[Connection]:
        aurora_read_replicas = []
        for replica_index in range(len(directory.aurora_readers())):
            aurora_read_replicas.append(
                ConnectionFactory.connect_to_sync(
                    Engine.Aurora,
                    schema_name,
                    config,
                    directory,
                    autocommit,
                    replica_index,
                )
            )
        return aurora_read_replicas
