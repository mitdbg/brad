import logging
from typing import List

from .operator import Operator
from brad.blueprint.data.table import Column
from brad.blueprint.sql_gen.table import comma_separated_column_names_and_types
from brad.config.dbtype import DBType
from brad.data_sync.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

_REGISTER_TABLE_TEMPLATE = """
    CREATE EXTERNAL TABLE {table_name} ({columns})
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
    LOCATION 's3://{s3_bucket}/{s3_path}'
"""


class RegisterAthenaS3Table(Operator):
    """
    Registers a table with Athena (where the data is already present on S3).
    """

    def __init__(
        self, table_name: str, columns: List[Column], relative_s3_path: str
    ) -> None:
        """
        NOTE: All S3 paths are relative to the extract path, specified in the
        configuration.
        """
        super().__init__()
        self._table_name = table_name
        self._columns = columns
        self._relative_s3_path = relative_s3_path

    async def execute(self, ctx: ExecutionContext) -> "Operator":
        query = _REGISTER_TABLE_TEMPLATE.format(
            table_name=self._table_name,
            columns=comma_separated_column_names_and_types(
                self._columns, DBType.Athena
            ),
            s3_bucket=ctx.s3_bucket(),
            s3_path="{}{}".format(ctx.s3_path(), self._relative_s3_path),
        )
        logger.debug("Running on Athena: %s", query)
        athena = await ctx.athena()
        await athena.execute(query)
        return self
