import logging

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.dbtype import DBType

logger = logging.getLogger(__name__)

_AURORA_UNLOAD_TEMPLATE = """
    SELECT * FROM aws_s3.query_export_to_s3(
        'SELECT * FROM {table_name}',
        aws_commons.create_s3_uri(
            '{s3_bucket}',
            '{s3_path}',
            '{s3_region}'
        )
    );
"""

# N.B. For larger unloads we will want PARALLEL ON. But our current sync
# implementation does not work with multiple data files stored under a
# prefix (need to add Redshift support for loading multiple files).
_REDSHIFT_UNLOAD_TEMPLATE = """
    UNLOAD ('SELECT * FROM {table_name}') TO 's3://{s3_bucket}/{s3_path}'
    IAM_ROLE '{s3_iam_role}'
    DELIMITER '|'
    PARALLEL OFF
"""


class UnloadToS3(Operator):
    """
    Dumps data from a table onto S3.
    """

    def __init__(self, table_name: str, relative_s3_path: str, engine: DBType) -> None:
        """
        NOTE: All S3 paths are relative to the extract path, specified in the
        configuration.
        """
        super().__init__()
        self._table_name = table_name
        self._engine = engine
        self._relative_s3_path = relative_s3_path

    def __repr__(self) -> str:
        return "".join(
            [
                "UnloadToS3(table_name=",
                self._table_name,
                ", engine=",
                self._engine,
                ", to=",
                self._relative_s3_path,
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        if self._engine == DBType.Aurora:
            return await self._execute_aurora(ctx)
        elif self._engine == DBType.Redshift:
            return await self._execute_redshift(ctx)
        else:
            # N.B. Athena is not supported.
            raise RuntimeError("Unsupported engine {}".format(self._engine))

    async def _execute_aurora(self, ctx: ExecutionContext) -> "Operator":
        query = _AURORA_UNLOAD_TEMPLATE.format(
            table_name=self._table_name,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
        )
        logger.debug("Running on Aurora: %s", query)
        aurora = await ctx.aurora()
        await aurora.execute(query)
        return self

    async def _execute_redshift(self, ctx: ExecutionContext) -> "Operator":
        query = _REDSHIFT_UNLOAD_TEMPLATE.format(
            table_name=self._table_name,
            s3_bucket=ctx.s3_bucket(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
            s3_iam_role=ctx.config().redshift_s3_iam_role,
        )
        logger.debug("Running on Redshift: %s", query)
        redshift = await ctx.redshift()
        await redshift.execute(query)
        return self
