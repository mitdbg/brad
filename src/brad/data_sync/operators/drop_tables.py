import logging
from typing import List

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.dbtype import DBType

logger = logging.getLogger(__name__)


class DropTables(Operator):
    """
    Drops tables on the given engine with the given names.
    """

    def __init__(self, table_names: List[str], engine: DBType) -> None:
        super().__init__()
        self._table_names = table_names
        self._engine = engine

    def __repr__(self) -> str:
        return "".join(
            [
                "DropTables(engine=",
                self._engine,
                ", ",
                ", ".join(self._table_names),
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        query_template = "DROP TABLE {}"

        if self._engine == DBType.Aurora:
            for table in self._table_names:
                query = query_template.format(table)
                logger.debug("Running on Aurora: %s", query)
                aurora = await ctx.aurora()
                await aurora.execute(query)

        elif self._engine == DBType.Redshift:
            for table in self._table_names:
                query = query_template.format(table)
                logger.debug("Running on Redshift: %s", query)
                redshift = await ctx.redshift()
                await redshift.execute(query)

        elif self._engine == DBType.Athena:
            for table in self._table_names:
                query = query_template.format(table)
                logger.debug("Running on Athena: %s", query)
                athena = await ctx.athena()
                await athena.execute(query)

        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

        return self
