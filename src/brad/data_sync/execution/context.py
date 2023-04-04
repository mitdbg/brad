import boto3
from typing import List

from brad.config.file import ConfigFile
from brad.blueprint.data import DataBlueprint


class ExecutionContext:
    def __init__(
        self,
        aurora,
        athena,
        redshift,
        blueprint: DataBlueprint,
        config: ConfigFile,
        s3_bucket: str,
        s3_region: str,
        s3_path: str,
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
        self._s3_bucket = s3_bucket
        self._s3_region = s3_region
        self._s3_path = s3_path

        # Extracted tables.
        self._extracted_tables: List[str] = []

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

    def blueprint(self) -> DataBlueprint:
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

    def set_extracted_tables(self, tables: List[str]) -> None:
        self._extracted_tables = tables
