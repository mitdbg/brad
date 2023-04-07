import logging

from .operator import Operator
from brad.config.dbtype import DBType
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


class RunTransformation(Operator):
    """
    Run a transformation on an engine. The input/output tables are expected to
    be set up.
    """

    def __init__(self, transform: str, engine: DBType, for_table: str) -> None:
        super().__init__()
        self._transform = transform
        self._engine = engine
        self._for_table = for_table

    def __repr__(self) -> str:
        return "".join(
            [
                "RunTransformation(for_table=",
                self._for_table,
                ", engine=",
                self._engine,
                ")",
            ]
        )

    def engine(self) -> DBType:
        return self._engine

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        queries = self._transform.split(";")
        logger.debug("Will run %d queries as part of this transform.", len(queries))

        if self._engine == DBType.Aurora:
            for query in queries:
                logger.debug("Running transform query on Aurora: %s", query)
                aurora = await ctx.aurora()
                await aurora.execute(query)
        elif self._engine == DBType.Redshift:
            for query in queries:
                logger.debug("Running transform query on Redshift: %s", query)
                redshift = await ctx.redshift()
                await redshift.execute(query)
        elif self._engine == DBType.Athena:
            for query in queries:
                logger.debug("Running transform query on Athena: %s", query)
                athena = await ctx.athena()
                await athena.execute(query)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

        return self
