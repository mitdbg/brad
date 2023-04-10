import logging
from typing import List

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.blueprint.table import Column
from brad.blueprint.sql_gen.table import comma_separated_column_names_and_types
from brad.config.engine import Engine

logger = logging.getLogger(__name__)


class CreateTempTable(Operator):
    """
    Creates a table on the given engine with the given name and columns. This is
    used to create temporary tables used during a data sync (e.g., to hold deltas).
    """

    def __init__(self, table_name: str, columns: List[Column], engine: Engine) -> None:
        super().__init__()
        self._table_name = table_name
        self._columns = columns
        self._engine = engine

    def __repr__(self) -> str:
        return "".join(
            [
                "CreateTempTable(table_name=",
                self._table_name,
                ", engine=",
                self._engine,
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        if self._engine == Engine.Aurora:
            return await self._execute_aurora(ctx)
        elif self._engine == Engine.Redshift:
            return await self._execute_redshift(ctx)
        elif self._engine == Engine.Athena:
            return await self._execute_athena(ctx)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

    async def _execute_aurora(self, ctx: ExecutionContext) -> "Operator":
        template = "CREATE TEMPORARY TABLE {table_name} ({columns})"
        query = template.format(
            table_name=self._table_name,
            columns=comma_separated_column_names_and_types(self._columns, self._engine),
        )
        logger.debug("Running on Aurora: %s", query)
        aurora = await ctx.aurora()
        await aurora.execute(query)
        return self

    async def _execute_redshift(self, ctx: ExecutionContext) -> "Operator":
        template = "CREATE TEMPORARY TABLE {table_name} ({columns})"
        query = template.format(
            table_name=self._table_name,
            columns=comma_separated_column_names_and_types(self._columns, self._engine),
        )
        logger.debug("Running on Redshift: %s", query)
        redshift = await ctx.redshift()
        await redshift.execute(query)
        return self

    async def _execute_athena(self, ctx: ExecutionContext) -> "Operator":
        # Temporary tables are not supported on Athena.
        template = "CREATE TABLE {table_name} ({columns}) LOCATION '{s3_path}' TBLPROPERTIES ('table_type' = 'ICEBERG');"
        query = template.format(
            table_name=self._table_name,
            columns=comma_separated_column_names_and_types(self._columns, self._engine),
            s3_path="s3://{}/{}/brad_athena_workspace/{}/".format(
                ctx.s3_bucket(),
                ctx.s3_path(),
                self._table_name,
            ),
        )
        logger.debug("Running on Athena: %s", query)
        athena = await ctx.athena()
        await athena.execute(query)
        return self
