import asyncio
import logging

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


class RunCommit(Operator):
    async def execute(self, ctx: ExecutionContext) -> "Operator":
        aurora = await ctx.aurora()
        redshift = await ctx.redshift()
        await asyncio.gather(aurora.commit(), redshift.commit())
        return self
