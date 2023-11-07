import logging
from typing import List

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.engine import Engine

logger = logging.getLogger(__name__)


class DropViews(Operator):
    """
    Drops views on the given engine with the given names.
    """

    def __init__(self, view_names: List[str], engine: Engine) -> None:
        super().__init__()
        self._view_names = view_names
        self._engine = engine

    def __repr__(self) -> str:
        return "".join(
            [
                "DropViews(engine=",
                self._engine,
                ", ",
                ", ".join(self._view_names),
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        query_template = "DROP VIEW {}"

        if self._engine == Engine.Aurora:
            for view in self._view_names:
                query = query_template.format(view)
                logger.debug("Running on Aurora: %s", query)
                aurora = await ctx.aurora()
                await aurora.execute(query)

        elif self._engine == Engine.Redshift:
            for view in self._view_names:
                query = query_template.format(view)
                logger.debug("Running on Redshift: %s", query)
                redshift = await ctx.redshift()
                await redshift.execute(query)

        elif self._engine == Engine.Athena:
            for view in self._view_names:
                query = query_template.format(view)
                logger.debug("Running on Athena: %s", query)
                athena = await ctx.athena()
                await athena.execute(query)

        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

        return self
