import asyncio
import boto3

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.server.engine_connections import EngineConnections


class TableSizer:
    """
    Utility class for retrieving the size of a table.
    """

    def __init__(self, engines: EngineConnections, config: ConfigFile) -> None:
        self._engines = engines
        self._config = config
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_access_key_secret,
        )

    async def table_size_mb(self, table_name: str, location: Engine) -> int:
        if location == Engine.Aurora:
            return await self._table_size_mb_aurora(table_name)
        elif location == Engine.Athena:
            return await self._table_size_mb_athena(table_name)
        elif location == Engine.Redshift:
            return await self._table_size_mb_redshift(table_name)
        else:
            raise RuntimeError(
                "Unknown location {} for table {}".format(str(location), table_name)
            )

    async def aurora_row_size_bytes(self, table_name: str) -> int:
        """
        A rough estimate for the size of a row in Aurora, in bytes.
        """

        query = f"SELECT pg_column_size(t.*) FROM {table_name} t LIMIT 1"
        conn = self._engines.get_connection(Engine.Aurora)
        cursor = await conn.cursor()
        await cursor.execute(query)
        row = await cursor.fetchone()
        return int(row[0])

    async def _table_size_mb_athena(self, table_name: str) -> int:
        # Format: s3://bucket/path/to/files/
        parts = self._config.athena_s3_data_path.split("/")
        bucket_name = parts[2]
        data_prefix = "/".join(parts[3:])
        table_prefix = "/" + data_prefix + table_name + "/"

        # NOTE: This may be problematic when there are many objects in a prefix.
        def run_inner():
            total_size_bytes = 0
            bucket = self._s3_client.Bucket(bucket_name)
            for obj in bucket.objects.filter(Prefix=table_prefix):
                total_size_bytes += obj.size
            # `total_size` is in bytes.
            return total_size_bytes

        loop = asyncio.get_running_loop()
        total_size_bytes = await loop.run_in_executor(None, run_inner)

        return int(total_size_bytes / 1000 / 1000)

    async def _table_size_mb_aurora(self, table_name: str) -> int:
        query = "SELECT pg_table_size('{}')".format(table_name)
        aurora = self._engines.get_connection(Engine.Aurora)
        cursor = await aurora.cursor()
        await cursor.execute(query)
        result = await cursor.fetchone()
        # The result is in bytes.
        return int(int(result[0]) / 1000 / 1000)

    async def _table_size_mb_redshift(self, table_name: str) -> int:
        query = "SELECT size FROM svv_table_info WHERE table = '{}';".format(table_name)
        redshift = self._engines.get_connection(Engine.Redshift)
        cursor = await redshift.cursor()
        await cursor.execute(query)
        result = await cursor.fetchone()
        table_size_mb = int(result[0])
        return table_size_mb
