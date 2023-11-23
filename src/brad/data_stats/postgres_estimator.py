import asyncio
import logging
from typing import Dict, List, Optional

from .estimator import Estimator

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.strings import (
    base_table_name_from_source,
    source_table_name,
    SIDECAR_DB_SIZE_TABLE,
)
from brad.connection.connection import Connection, ConnectionFailed
from brad.connection.cursor import Cursor
from brad.connection.factory import ConnectionFactory
from brad.data_stats.estimator import AccessInfo
from brad.data_stats.plan_parsing import (
    parse_explain_verbose,
    extract_base_cardinalities,
)
from brad.query_rep import QueryRep
from brad.utils.rand_exponential_backoff import RandomizedExponentialBackoff

logger = logging.getLogger(__name__)


class PostgresEstimator(Estimator):
    @classmethod
    async def connect(
        cls,
        schema_name: str,
        config: ConfigFile,
    ) -> "PostgresEstimator":
        connection = await ConnectionFactory.connect_to_sidecar(schema_name, config)
        return cls(connection, await connection.cursor(), schema_name, config)

    def __init__(
        self,
        connection: Connection,
        cursor: Cursor,
        schema_name: str,
        config: ConfigFile,
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._schema_name = schema_name
        self._config = config

        self._blueprint: Optional[Blueprint] = None
        self._table_sizes: Dict[str, int] = {}
        self._reconnect_lock = asyncio.Lock()

    async def analyze(
        self, blueprint: Blueprint, populate_cache_if_missing: bool = False
    ) -> None:
        self._blueprint = blueprint
        self._table_sizes.clear()
        self._table_sizes.update(await self._get_table_sizes(populate_cache_if_missing))

    async def get_access_info(self, query: QueryRep) -> List[AccessInfo]:
        attempts = 0
        while attempts < 10:
            try:
                return await self._get_access_info_impl(query)
            except Exception as ex:
                if not self._connection.is_connection_lost_error(ex):
                    raise
                else:
                    self._connection.mark_connection_lost()

            # Try to reconnect.
            attempts += 1
            await self._try_reconnect()

        raise RuntimeError(
            "Fatal error: Unable to estimate cardinalities due to a lost connection to the sidecar DB."
        )

    async def _get_access_info_impl(self, query: QueryRep) -> List[AccessInfo]:
        explain_query = f"EXPLAIN VERBOSE {query.raw_query}"
        await self._cursor.execute(explain_query)
        plan_lines = [row[0] async for row in self._cursor]
        return self._extract_access_infos(plan_lines)

    def get_access_info_sync(self, query: QueryRep) -> List[AccessInfo]:
        explain_query = f"EXPLAIN VERBOSE {query.raw_query}"
        self._cursor.execute_sync(explain_query)
        plan_lines = [row[0] for row in self._cursor]
        return self._extract_access_infos(plan_lines)

    async def close(self) -> None:
        await self._connection.close()

    async def _try_reconnect(self) -> None:
        # This is meant to deal with intermittent lost connections. This may
        # happen if the sidecar is also the main Aurora DB, which is restarted
        # during a provisioning change.
        async with self._reconnect_lock:
            if self._connection.is_connected():
                return

            backoff = None
            while True:
                try:
                    logger.debug("Attempting to reconnect to the sidecar DB...")
                    connection = await ConnectionFactory.connect_to_sidecar(
                        self._schema_name, self._config
                    )
                    cursor = await connection.cursor()
                    self._connection = connection
                    self._cursor = cursor
                    return
                except ConnectionFailed:
                    pass

                if backoff is None:
                    backoff = RandomizedExponentialBackoff(
                        max_retries=10, base_delay_s=2, max_delay_s=5 * 60
                    )
                wait_s = backoff.wait_time_s()
                if wait_s is None:
                    raise RuntimeError("Failed to reconnect to the sidecar DB.")
                await asyncio.sleep(wait_s)

    async def _get_table_sizes(self, populate_cache_if_missing: bool) -> Dict[str, int]:
        # Try using previously cached results (faster).
        table_counts = await self._get_table_sizes_from_cache()
        if table_counts is not None:
            return table_counts

        # Fallback to direct stats.
        table_counts = await self._get_table_sizes_direct()

        if populate_cache_if_missing:
            await self._cache_table_sizes(table_counts)

        return table_counts

    def _extract_access_infos(self, plan_lines: List[str]) -> List[AccessInfo]:
        parsed_plan = parse_explain_verbose(plan_lines)
        base_cards = extract_base_cardinalities(parsed_plan)

        access_infos = []
        for bc in base_cards:
            table_name = base_table_name_from_source(bc.table_name)
            if table_name not in self._table_sizes:
                logger.warning(
                    "Missing table in the table sizes cache: %s (raw name)",
                    bc.table_name,
                )
                continue
            access_infos.append(
                AccessInfo(
                    table_name,
                    bc.cardinality,
                    bc.cardinality / self._table_sizes[table_name],
                    bc.width,
                    bc.access_op_name,
                )
            )

        return access_infos

    async def _get_table_sizes_stats(self) -> Optional[Dict[str, int]]:
        assert self._blueprint is not None

        # Fetch stats from PostgreSQL directly.
        raw_table_counts = {}
        query = "SELECT relname, n_live_tup FROM pg_stat_user_tables"
        await self._cursor.execute(query)
        results = await self._cursor.fetchall()
        for row in results:
            raw_table_name = row[0]
            row_count = row[1]
            raw_table_counts[raw_table_name] = row_count

        table_counts = {}
        for table in self._blueprint.tables():
            source_name = source_table_name(table)
            if source_name not in raw_table_counts:
                logger.debug("Missing tables when trying to use PostgreSQL stats.")
                return None
            table_counts[table.name] = raw_table_counts[source_name]

        if all(map(lambda v: v == 0, table_counts.values())):
            logger.debug("Table stats are all zero.")
            return None

        logger.debug("Using table sizes from pg_stat_user_tables.")
        return table_counts

    async def _get_table_sizes_direct(self) -> Dict[str, int]:
        assert self._blueprint is not None
        table_counts = {}

        for table in self._blueprint.tables():
            locations = self._blueprint.get_table_locations(table.name)
            if Engine.Aurora not in locations:
                logger.warning("Not fetching size of %s.", table.name)
                continue
            query = f"SELECT COUNT(*) FROM {table.name}"
            logger.debug("PostgresEstimator running: %s", query)
            await self._cursor.execute(query)
            row = await self._cursor.fetchone()
            assert row is not None
            table_counts[table.name] = int(row[0])

        return table_counts

    async def _get_table_sizes_from_cache(self) -> Optional[Dict[str, int]]:
        try:
            cursor = await self._connection.cursor()
            await cursor.execute(
                "SELECT table_name, row_count FROM {}".format(SIDECAR_DB_SIZE_TABLE)
            )
            results = await cursor.fetchall()
            return {row[0]: int(row[1]) for row in results}
        except:  # pylint: disable=bare-except
            logger.exception(
                "Failed to retrieve table sizes from the cache. Will recompute."
            )
            return None

    async def _cache_table_sizes(self, table_sizes: Dict[str, int]) -> None:
        cursor = await self._connection.cursor()
        try:
            await cursor.execute("BEGIN")
            await cursor.execute(
                "CREATE TABLE IF NOT EXISTS {} (table_name TEXT PRIMARY KEY, row_count BIGINT NOT NULL)".format(
                    SIDECAR_DB_SIZE_TABLE
                )
            )
            await cursor.execute("TRUNCATE TABLE {}".format(SIDECAR_DB_SIZE_TABLE))
            for table_name, row_count in table_sizes.items():
                await cursor.execute(
                    "INSERT INTO {} (table_name, row_count) VALUES ('{}', {})".format(
                        SIDECAR_DB_SIZE_TABLE, table_name, row_count
                    )
                )
            await cursor.execute("COMMIT")
        except:
            await cursor.execute("ROLLBACK")
            raise
