import logging

from .operator import Operator
from brad.blueprint.sql_gen.table import comma_separated_column_names
from brad.config.engine import Engine
from brad.config.strings import insert_delta_table_name, delete_delta_table_name
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


class AdjustDeltas(Operator):
    """
    Adjusts the deltas before running a transformation. This operator must run
    on the raw deltas extracted from Aurora before they can be passed to a
    transformation function.

    More details:
    Aurora's deltas do not differentiate inserts and updates. Since our delta
    application semantics are to apply deletes and then inserts, we add all
    Aurora writes to the deletes delta as well.
    """

    def __init__(self, table_name: str, engine: Engine) -> None:
        super().__init__()
        self._table_name = table_name
        self._engine = engine

    def __repr__(self) -> str:
        return "".join(
            [
                "AdjustDeltas(table_name=",
                self._table_name,
                ", engine=",
                self._engine,
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        table = ctx.blueprint().get_table(self._table_name)
        query = "INSERT INTO {delete_deltas} SELECT {pkey_cols} FROM {insert_deltas}".format(
            delete_deltas=delete_delta_table_name(table),
            insert_deltas=insert_delta_table_name(table),
            pkey_cols=comma_separated_column_names(table.primary_key),
        )

        if self._engine == Engine.Aurora:
            logger.debug("Running on Aurora: %s", query)
            aurora = await ctx.aurora()
            await aurora.execute(query)
        elif self._engine == Engine.Redshift:
            logger.debug("Running on Redshift: %s", query)
            redshift = await ctx.redshift()
            await redshift.execute(query)
        elif self._engine == Engine.Athena:
            logger.debug("Running on Athena: %s", query)
            athena = await ctx.athena()
            await athena.execute(query)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

        return self
