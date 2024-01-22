import boto3
import logging

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.strings import source_table_name
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class TableSizer:
    """
    Utility class for retrieving the size of a table.
    """

    def __init__(self, engines: EngineConnections, config: ConfigFile) -> None:
        self._engines = engines
        self._config = config
        self._s3 = boto3.resource(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_access_key_secret,
        )

    def table_size_mb(self, table_name: str, location: Engine) -> int:
        if location == Engine.Aurora:
            return self._table_size_mb_aurora(table_name)
        elif location == Engine.Athena:
            return self._table_size_mb_athena(table_name)
        elif location == Engine.Redshift:
            return self._table_size_mb_redshift(table_name)
        else:
            raise RuntimeError(
                "Unknown location {} for table {}".format(str(location), table_name)
            )

    def table_size_bytes(self, table_name: str, location: Engine) -> int:
        if location == Engine.Aurora:
            return self._table_size_bytes_aurora(table_name)
        elif location == Engine.Athena:
            return self._table_size_bytes_athena(table_name)
        elif location == Engine.Redshift:
            return self._table_size_bytes_redshift(table_name)
        else:
            raise RuntimeError(
                "Unknown location {} for table {}".format(str(location), table_name)
            )

    async def table_size_rows(
        self, table_name: str, location: Engine, approximate_allowed: bool = False
    ) -> int:
        # NOTE: Special case.
        if table_name == "embeddings" and location != Engine.Aurora:
            return 1

        if location == Engine.Aurora:
            conn = self._engines.get_connection(Engine.Aurora)
        elif location == Engine.Redshift:
            conn = self._engines.get_connection(Engine.Redshift)
        elif location == Engine.Athena:
            conn = self._engines.get_connection(Engine.Athena)

        if approximate_allowed and location == Engine.Aurora:
            # Special case since SELECT COUNT(*) on Aurora is generally slow.
            query = f"SELECT reltuples FROM pg_class WHERE relname = '{source_table_name(table_name)}'"
            cursor = conn.cursor_sync()
            await cursor.execute(query)
            row = cursor.fetchone_sync()
            assert row is not None, table_name
            result = int(row[0])
            if result < 0:
                logger.warning(
                    "Extracted a negative table size for %s from %s: %d. Falling back to slower scan.",
                    table_name,
                    location,
                    result,
                )
            else:
                logger.debug("Using approximate size of %s: %d", table_name, result)
                return result

        query = f"SELECT COUNT(*) FROM {table_name}"
        cursor = conn.cursor_sync()
        await cursor.execute(query)
        row = cursor.fetchone_sync()
        assert row is not None, table_name
        result = int(row[0])
        return result

    def aurora_row_size_bytes(self, table_name: str) -> int:
        """
        A rough estimate for the size of a row in Aurora, in bytes.
        """

        query = f"SELECT pg_column_size(t.*) FROM {table_name} t LIMIT 1"
        conn = self._engines.get_connection(Engine.Aurora)
        cursor = conn.cursor_sync()
        cursor.execute_sync(query)
        row = cursor.fetchone_sync()
        assert row is not None
        return int(row[0])

    def _table_size_mb_athena(self, table_name: str) -> int:
        return int(self._table_size_bytes_athena(table_name) / 1000 / 1000)

    def _table_size_bytes_athena(self, table_name: str) -> int:
        # Format: s3://bucket/path/to/files/
        parts = self._config.athena_s3_data_path.split("/")
        bucket_name = parts[2]
        data_prefix = "/".join(parts[3:])
        table_prefix = data_prefix + table_name + "/"

        # NOTE: This may be problematic when there are many objects in a prefix.
        def run_inner():
            total_size_bytes = 0
            bucket = self._s3.Bucket(bucket_name)
            for obj in bucket.objects.filter(Prefix=table_prefix):
                total_size_bytes += obj.size
            # `total_size` is in bytes.
            return total_size_bytes

        total_size_bytes = run_inner()
        return total_size_bytes

    def _table_size_mb_aurora(self, table_name: str) -> int:
        return int(self._table_size_bytes_aurora(table_name) / 1000 / 1000)

    def _table_size_bytes_aurora(self, table_name: str) -> int:
        query = "SELECT pg_table_size('{}')".format(source_table_name(table_name))
        aurora = self._engines.get_connection(Engine.Aurora)
        cursor = aurora.cursor_sync()
        logger.debug("Running on Aurora: %s", query)
        cursor.execute_sync(query)
        result = cursor.fetchone_sync()
        assert result is not None
        # The result is in bytes.
        return int(result[0])

    def _table_size_mb_redshift(self, table_name: str) -> int:
        query = "SELECT size FROM svv_table_info WHERE \"table\" = '{}';".format(
            table_name
        )
        redshift = self._engines.get_connection(Engine.Redshift)
        logger.debug("Running on Redshift: %s", query)
        cursor = redshift.cursor_sync()
        cursor.execute_sync(query)
        result = cursor.fetchone_sync()
        assert result is not None
        table_size_mb = int(result[0])
        return table_size_mb

    def _table_size_bytes_redshift(self, table_name: str) -> int:
        return self._table_size_mb_redshift(table_name) * 1000 * 1000
