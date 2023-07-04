from typing import Dict, List, Optional

from .estimator import Estimator

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.config.strings import base_table_name_from_source
from brad.connection.connection import Connection
from brad.connection.cursor import Cursor
from brad.connection.factory import ConnectionFactory
from brad.data_stats.estimator import AccessInfo
from brad.data_stats.plan_parsing import (
    parse_explain_verbose,
    extract_base_cardinalities,
)
from brad.query_rep import QueryRep


class PostgresEstimator(Estimator):
    @classmethod
    async def connect(
        cls,
        schema_name: str,
        config: ConfigFile,
    ) -> "PostgresEstimator":
        connection = await ConnectionFactory.connect_to_sidecar(schema_name, config)
        return cls(connection, await connection.cursor(), schema_name)

    def __init__(
        self, connection: Connection, cursor: Cursor, schema_name: str
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._schema_name = schema_name

        self._blueprint: Optional[Blueprint] = None
        self._table_sizes: Dict[str, int] = {}

    async def analyze(self, blueprint: Blueprint) -> None:
        self._blueprint = blueprint
        self._table_sizes.clear()
        self._table_sizes.update(await self._get_table_sizes())

    async def get_access_info(self, query: QueryRep) -> List[AccessInfo]:
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

    async def _get_table_sizes(self) -> Dict[str, int]:
        assert self._blueprint is not None
        table_counts = {}

        for table in self._blueprint.tables():
            await self._cursor.execute(f"SELECT COUNT(*) FROM {table.name}")
            row = await self._cursor.fetchone()
            assert row is not None
            table_counts[table.name] = int(row[0])

        return table_counts

    def _extract_access_infos(self, plan_lines: List[str]) -> List[AccessInfo]:
        parsed_plan = parse_explain_verbose(plan_lines)
        base_cards = extract_base_cardinalities(parsed_plan)

        access_infos = []
        for bc in base_cards:
            table_name = base_table_name_from_source(bc.table_name)
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
