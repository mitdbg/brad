import logging

from .operator import Operator
from brad.data_sync.execution.context import ExecutionContext
from brad.config.engine import Engine
from brad.blueprint.sql_gen.table import comma_separated_column_names_and_types
from brad.config.strings import source_table_name

logger = logging.getLogger(__name__)

# N.B. Can only load into a table with the exact same schema as the data present
# on S3.
_AURORA_LOAD_TEMPLATE = """
    SELECT aws_s3.table_import_from_s3(
        '{table_name}',
        '{aurora_columns}',
        '(FORMAT csv, DELIMITER ''{delimiter}''{header_str})',
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
    DELIMITER '{delimiter}'
    IGNOREHEADER {header_rows}
    REMOVEQUOTES
    BLANKASNULL
    IGNOREALLERRORS
"""

_ATHENA_CREATE_LOAD_TABLE = """
    CREATE EXTERNAL TABLE {load_table_name} ({columns})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '{delimiter}' STORED AS TEXTFILE
    LOCATION 's3://{s3_bucket}/{s3_path}'
    TBLPROPERTIES ('skip.header.line.count' = '{header_rows}')
"""


class LoadFromS3(Operator):
    """
    Loads data (stored on S3) into a table on an engine.
    """

    def __init__(
        self,
        table_name: str,
        relative_s3_path: str,
        engine: Engine,
        delimiter: str = "|",
        header_rows: int = 0,
        aurora_columns: str = "",
    ) -> None:
        """
        NOTE: All S3 paths are relative to the extract path, specified in the
        configuration.
        """
        super().__init__()
        self._table_name = table_name
        self._engine = engine
        self._relative_s3_path = relative_s3_path
        self._delimiter = delimiter
        self._header_rows = header_rows
        self._aurora_columns = aurora_columns

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
        elif self._engine == Engine.Athena:
            return await self._execute_athena(ctx)
        else:
            raise RuntimeError("Unsupported engine {}".format(self._engine))

    async def _execute_aurora(self, ctx: ExecutionContext) -> "Operator":
        query = _AURORA_LOAD_TEMPLATE.format(
            table_name=self._table_name,
            aurora_columns=self._aurora_columns,
            s3_bucket=ctx.s3_bucket(),
            s3_region=ctx.s3_region(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
            delimiter=self._delimiter,
            header_str=", HEADER" if self._header_rows > 0 else "",
        )
        logger.debug("Running on Aurora: %s", query)
        aurora = await ctx.aurora()
        await aurora.execute(query)

        # Reset the next sequence values for SERIAL/BIGSERIAL types after loading
        # (Aurora does not automatically update it).
        table = ctx.blueprint().get_table(self._table_name)
        for column in table.columns:
            if column.data_type != "SERIAL" and column.data_type != "BIGSERIAL":
                continue
            query = "SELECT MAX({}) FROM {}".format(
                column.name, source_table_name(table)
            )
            logger.debug("Running on Aurora: %s", query)
            await aurora.execute(query)
            row = await aurora.fetchone()
            if row is None:
                continue
            max_serial_val = row[0]
            query = "ALTER SEQUENCE {}_{}_seq RESTART WITH {}".format(
                source_table_name(table), column.name, str(max_serial_val + 1)
            )
            logger.debug("Running on Aurora: %s", query)
            await aurora.execute(query)

        return self

    async def _execute_redshift(self, ctx: ExecutionContext) -> "Operator":
        query = _REDSHIFT_LOAD_TEMPLATE.format(
            table_name=self._table_name,
            s3_bucket=ctx.s3_bucket(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
            s3_iam_role=ctx.config().redshift_s3_iam_role,
            delimiter=self._delimiter,
            header_rows=str(self._header_rows),
        )
        logger.debug("Running on Redshift: %s", query)
        redshift = await ctx.redshift()
        await redshift.execute(query)
        return self

    async def _execute_athena(self, ctx: ExecutionContext) -> "Operator":
        # TODO: this duplicates code from `brad.admin.bulk_load._load_athena()`
        # Consider refactoring, possibly by having bulk load call into functionlaity from here.

        table = ctx.blueprint().get_table(self._table_name)

        # 1. We need to create a loading table.
        query = _ATHENA_CREATE_LOAD_TABLE.format(
            load_table_name="{}_brad_loading".format(self._table_name),
            columns=comma_separated_column_names_and_types(
                table.columns, Engine.Athena
            ),
            s3_bucket=ctx.s3_bucket(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
            delimiter=self._delimiter,
            header_rows=str(self._header_rows),
        )
        logger.debug("Running on Athena %s", query)
        athena = await ctx.athena()
        await athena.execute(query)

        # 2. Actually run the load.
        query = (
            "INSERT INTO {table_name} SELECT * FROM {table_name}_brad_loading".format(
                table_name=self._table_name
            )
        )
        logger.debug("Running on Athena %s", query)
        await athena.execute(query)

        # 3. Remove the loading table.
        q = "DROP TABLE {}_brad_loading".format(self._table_name)
        logger.debug("Running on Athena %s", q)
        await athena.execute(q)

        logger.info("Done loading %s on Athena!", self._table_name)
        return self
