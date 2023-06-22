import asyncio
import boto3

from brad.config.file import ConfigFile


class AssetManager:
    """
    Used to retrieve/persist large assets from S3. The methods in this class
    assume the assets can fit in memory.
    """

    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=self._config.aws_access_key,
            aws_secret_access_key=self._config.aws_access_key_secret,
        )

    def load_sync(self, key: str) -> bytes:
        response = self._s3_client.get_object(
            Bucket=self._config.s3_assets_bucket,
            Key=self._config.s3_assets_path + key,
        )
        return response["Body"].read()

    def persist_sync(self, key: str, payload: bytes) -> None:
        self._s3_client.put_object(
            Body=payload,
            Bucket=self._config.s3_assets_bucket,
            Key=self._config.s3_assets_path + key,
        )

    def delete_sync(self, key: str) -> None:
        self._s3_client.delete_object(
            Bucket=self._config.s3_assets_bucket,
            Key=self._config.s3_assets_path + key,
        )

    async def load(self, key: str) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.load_sync, key)

    async def persist(self, key: str, payload: bytes) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.persist_sync, key, payload)
