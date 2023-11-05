import logging
from typing import List

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.strings import (
    source_table_name,
    update_trigger_name,
    delete_trigger_name,
    update_trigger_function_name,
    delete_trigger_function_name,
)

logger = logging.getLogger(__name__)


class DropAuroraTriggers(Operator):
    """
    Drops the Aurora triggers and trigger functions associated with specific tables.
    """

    def __init__(self, table_names: List[str]) -> None:
        super().__init__()
        self._table_names = table_names

    def __repr__(self) -> str:
        return "".join(
            [
                "DropAuroraTriggers(",
                ", ".join(self._table_names),
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        drop_trigger_template = "DROP TRIGGER {} ON {}"
        drop_trigger_fn_template = "DROP FUNCTION {}"

        for table in self._table_names:
            # Drop update trigger
            q = drop_trigger_template.format(
                update_trigger_name(table), source_table_name(table)
            )
            logger.debug("Running on Aurora: %s", q)
            aurora = await ctx.aurora()
            await aurora.execute(q)

            # Drop delete trigger
            q = drop_trigger_template.format(
                delete_trigger_name(table), source_table_name(table)
            )
            logger.debug("Running on Aurora: %s", q)
            aurora = await ctx.aurora()
            await aurora.execute(q)

            # Drop update trigger function
            q = drop_trigger_fn_template.format(update_trigger_function_name(table))
            logger.debug("Running on Aurora: %s", q)
            aurora = await ctx.aurora()
            await aurora.execute(q)

            # Drop delete trigger function
            q = drop_trigger_fn_template.format(delete_trigger_function_name(table))
            logger.debug("Running on Aurora: %s", q)
            aurora = await ctx.aurora()
            await aurora.execute(q)

        return self
