import logging
from typing import Optional

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.engine import Engine

logger = logging.getLogger(__name__)

_AURORA_UNLOAD_TEMPLATE = """
    SELECT * FROM aws_s3.query_export_to_s3(
        '{query}',
        aws_commons.create_s3_uri(
            '{s3_bucket}',
            '{s3_path}',
            '{s3_region}'
        ),
        'format csv, delimiter $${delimiter}$$, header true'
    );
"""

# N.B. For larger unloads we will want PARALLEL ON. But our current sync
# implementation does not work with multiple data files stored under a
# prefix (need to add Redshift support for loading multiple files).
_REDSHIFT_UNLOAD_TEMPLATE = """
    UNLOAD ('SELECT * FROM {table_name}') TO 's3://{s3_bucket}/{s3_path}'
    IAM_ROLE '{s3_iam_role}'
    DELIMITER '{delimiter}'
    ALLOWOVERWRITE
    HEADER
    PARALLEL OFF
"""

_ATHENA_UNLOAD_TEMPLATE = """
    SELECT * FROM {table_name}
"""


class UnloadToS3(Operator):
    """
    Dumps data from a table onto S3.
    """

    def __init__(
        self,
        table_name: str,
        relative_s3_path: str,
        engine: Engine,
        limit: Optional[int] = None,
        delimiter: str = "|",
    ) -> None:
        """
        NOTE: All S3 paths are relative to the extract path, specified in the
        configuration.
        """
        super().__init__()
        self._table_name = table_name
        self._engine = engine
        self._relative_s3_path = relative_s3_path
        self._limit = limit
        self._delimiter = delimiter

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
        if self._engine == Engine.Aurora:
            return await self._execute_aurora(ctx)
        elif self._engine == Engine.Redshift:
            return await self._execute_redshift(ctx)
        elif self._engine == Engine.Athena:
            return await self._execute_athena(ctx)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

    async def _execute_aurora(self, ctx: ExecutionContext) -> "Operator":
        inner_query = f"SELECT * FROM {self._table_name}"
        if self._limit is not None:
            inner_query += f" LIMIT {self._limit}"

        query = _AURORA_UNLOAD_TEMPLATE.format(
            query=inner_query,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
            delimiter=self._delimiter,
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
            delimiter=self._delimiter,
        )
        logger.debug("Running on Redshift: %s", query)
        redshift = await ctx.redshift()
        await redshift.execute(query)
        return self

    async def _execute_athena(self, ctx: ExecutionContext) -> "Operator":
        query = _ATHENA_UNLOAD_TEMPLATE.format(table_name=self._table_name)
        logger.debug("Running on Athena: %s", query)
        athena = await ctx.athena()
        await athena.execute(query)

        # Query results will have been saved as a csv in the `s3_output_path`.
        # Let's copy them, using "the most recent csv file" as our target.
        # TODO: possibly make this less hacky.

        athena_output_prefix = ctx.athena_s3_output_path()[
            (len("s3://") + len(ctx.s3_bucket()) + 1) :
        ]  # Remove the bucket name from the path

        objects = ctx.s3_client().list_objects_v2(
            Bucket=ctx.s3_bucket(),
            Prefix=athena_output_prefix,
        )
        most_recent_csv_file = max(
            (obj for obj in objects.get("Contents", []) if obj["Key"].endswith(".csv")),
            key=lambda x: x["LastModified"],
            default=None,
        )

        if most_recent_csv_file:
            ctx.s3_client().copy_object(
                CopySource={
                    "Bucket": ctx.s3_bucket(),
                    "Key": most_recent_csv_file["Key"],
                },
                Bucket=ctx.s3_bucket(),
                Key=f"{ctx.s3_path()}transition/{self._table_name}/{self._table_name}.tbl",
            )

            logger.debug(
                f"Extracted table to {ctx.s3_path()}transition/{self._table_name}/{self._table_name}.tbl"
            )
        else:
            logger.error(
                f"Could not extract table {self._table_name} from Athena to S3."
            )

        return self
