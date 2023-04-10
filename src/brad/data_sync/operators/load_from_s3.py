import logging

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.engine import Engine

logger = logging.getLogger(__name__)

# N.B. Can only load into a table with the exact same schema as the data present
# on S3.
_AURORA_LOAD_TEMPLATE = """
    SELECT aws_s3.table_import_from_s3(
        '{table_name}',
        '',
        '(FORMAT text, DELIMITER ''|'')',
        aws_commons.create_s3_uri(
            '{s3_bucket}',
            '{s3_path}',
            '{s3_region}'
        )
    );
"""

_REDSHIFT_LOAD_TEMPLATE = """
    COPY {table_name} FROM 's3://{s3_bucket}/{s3_path}'
    IAM_ROLE '{s3_iam_role}'
    DELIMITER '|'
"""


class LoadFromS3(Operator):
    """
    Loads data (stored on S3) into a table on an engine.
    """

    def __init__(self, table_name: str, relative_s3_path: str, engine: Engine) -> None:
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
                "LoadFromS3(table_name=",
                self._table_name,
                ", engine=",
                self._engine,
                ", s3_path=",
                self._relative_s3_path,
                ")",
            ]
        )

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        if self._engine == Engine.Aurora:
            return await self._execute_aurora(ctx)
        elif self._engine == Engine.Redshift:
            return await self._execute_redshift(ctx)
        else:
            # N.B. Athena is not supported.
            raise RuntimeError("Unsupported engine {}".format(self._engine))

    async def _execute_aurora(self, ctx: ExecutionContext) -> "Operator":
        query = _AURORA_LOAD_TEMPLATE.format(
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
        query = _REDSHIFT_LOAD_TEMPLATE.format(
            table_name=self._table_name,
            s3_bucket=ctx.s3_bucket(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
            s3_iam_role=ctx.config().redshift_s3_iam_role,
        )
        logger.debug("Running on Redshift: %s", query)
        redshift = await ctx.redshift()
        await redshift.execute(query)
        return self
