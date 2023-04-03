import logging

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.dbtype import DBType

logger = logging.getLogger(__name__)


class DropTable(Operator):
    """
    Drops a table on the given engine with the given name.
    """

    def __init__(self, table_name: str, engine: DBType) -> None:
        super().__init__()
        self._table_name = table_name
        self._engine = engine

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        query = "DROP TABLE {}".format(self._table_name)

        if self._engine == DBType.Aurora:
            logger.debug("Running on Aurora: %s", query)
            aurora = await ctx.aurora()
            await aurora.execute(query)
        elif self._engine == DBType.Redshift:
            logger.debug("Running on Redshift: %s", query)
            redshift = await ctx.redshift()
            await redshift.execute(query)
        elif self._engine == DBType.Athena:
            logger.debug("Running on Athena: %s", query)
            athena = await ctx.athena()
            await athena.execute(query)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

        return self
