import boto3
from typing import Dict, TYPE_CHECKING

from brad.blueprint import Blueprint
from brad.config.file import ConfigFile

# Needed to avoid a circular import.
if TYPE_CHECKING:
    from brad.data_sync.execution.table_sync_bounds import TableSyncBounds


class ExecutionContext:
    def __init__(
        self,
        aurora,
        athena,
        redshift,
        blueprint: Blueprint,
        config: ConfigFile,
    ) -> None:
        self._aurora = aurora
        self._aurora_cursor = None

        self._athena = athena
        self._athena_cursor = None

        self._redshift = redshift
        self._redshift_cursor = None

        self._blueprint = blueprint
        self._config = config

        # The "location" in S3 used for data sync intermediate results.
        self._s3_bucket = self._config.s3_extract_bucket
        self._s3_region = self._config.s3_extract_region
        self._s3_path = self._config.s3_extract_path

        # Table bounds (pre-computed) for extraction.
        self._table_bounds: Dict[str, "TableSyncBounds"] = {}

        # NOTE: We need to create one per thread.
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=self._config.aws_access_key,
            aws_secret_access_key=self._config.aws_access_key_secret,
        )

    async def aurora(self):
        """Connection to the Aurora engine."""
        if self._aurora_cursor is None:
            self._aurora_cursor = await self._aurora.cursor()
        return self._aurora_cursor

    async def athena(self):
        """Connection to the Athena engine."""
        if self._athena_cursor is None:
            self._athena_cursor = await self._athena.cursor()
        return self._athena_cursor

    async def redshift(self):
        """Connection to the Redshift engine."""
        if self._redshift_cursor is None:
            self._redshift_cursor = await self._redshift.cursor()
        return self._redshift_cursor

    def blueprint(self) -> Blueprint:
        return self._blueprint

    def s3_bucket(self) -> str:
        return self._s3_bucket

    def s3_region(self) -> str:
        return self._s3_region

    def s3_path(self) -> str:
        return self._s3_path

    def s3_client(self):
        return self._s3_client

    def config(self) -> ConfigFile:
        return self._config

    def table_sync_bounds(self) -> Dict[str, "TableSyncBounds"]:
        return self._table_bounds

    def set_table_sync_bounds(self, bounds: Dict[str, "TableSyncBounds"]) -> None:
        self._table_bounds = bounds
