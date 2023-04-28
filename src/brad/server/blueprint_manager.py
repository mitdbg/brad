import asyncio
import boto3
from typing import Any, Dict, Optional

from brad.blueprint import Blueprint
from brad.blueprint.serde import (
    serialize_blueprint,
    deserialize_blueprint,
)
from brad.config.file import ConfigFile

_METADATA_KEY_TEMPLATE = "{}{}.brad"


class BlueprintManager:
    """
    Utility class used for loading and providing access to the current blueprint.
    """

    def __init__(self, config: ConfigFile, schema_name: str):
        self._config = config
        self._schema_name = schema_name
        self._blueprint: Optional[Blueprint] = None
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=self._config.aws_access_key,
            aws_secret_access_key=self._config.aws_access_key_secret,
        )

    async def load(self) -> None:
        """
        Loads the persisted version of the blueprint from S3.
        """

        loop = asyncio.get_running_loop()

        def _get_s3() -> Dict[str, Any]:
            return self._s3_client.get_object(
                Bucket=self._config.s3_metadata_bucket,
                Key=_METADATA_KEY_TEMPLATE.format(
                    self._config.s3_metadata_path, self._schema_name
                ),
            )

        response = await loop.run_in_executor(
            None,
            _get_s3,
        )

        def _load_response() -> bytes:
            return response["Body"].read()

        serialized = await loop.run_in_executor(None, _load_response)
        self._blueprint = deserialize_blueprint(serialized)

    def load_sync(self) -> None:
        asyncio.run(self.load())

    def persist_sync(self) -> None:
        """
        Persists the current blueprint to S3.
        """

        assert self._blueprint is not None
        serialized = serialize_blueprint(self._blueprint)
        self._s3_client.put_object(
            Body=serialized,
            Bucket=self._config.s3_metadata_bucket,
            Key=_METADATA_KEY_TEMPLATE.format(
                self._config.s3_metadata_path, self._schema_name
            ),
        )

    def delete_sync(self) -> None:
        """
        Deletes the persisted blueprint from S3.
        """

        self._s3_client.delete_object(
            Bucket=self._config.s3_metadata_bucket,
            Key=_METADATA_KEY_TEMPLATE.format(
                self._config.s3_metadata_path, self._schema_name
            ),
        )

    @property
    def schema_name(self) -> str:
        return self._schema_name

    def get_blueprint(self) -> Blueprint:
        assert self._blueprint is not None
        return self._blueprint

    def set_blueprint(self, blueprint: Blueprint) -> None:
        self._blueprint = blueprint
