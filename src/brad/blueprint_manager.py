import asyncio
from typing import Optional

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.serde import (
    serialize_blueprint,
    deserialize_blueprint,
)
from brad.config.file import ConfigFile
from brad.provisioning.directory import Directory


class BlueprintManager:
    """
    Utility class used for loading and providing access to the current blueprint.
    """

    def __init__(
        self, config: ConfigFile, assets: AssetManager, schema_name: str
    ) -> None:
        self._assets = assets
        self._schema_name = schema_name
        self._blueprint: Optional[Blueprint] = None
        self._directory = Directory(config)

    async def load(self) -> None:
        """
        Loads the persisted version of the blueprint from S3.
        """
        serialized = await self._assets.load(
            _METADATA_KEY_TEMPLATE.format(self._schema_name)
        )
        self._blueprint = deserialize_blueprint(serialized)
        await self._directory.refresh()

    def load_sync(self) -> None:
        serialized = self._assets.load_sync(
            _METADATA_KEY_TEMPLATE.format(self._schema_name)
        )
        self._blueprint = deserialize_blueprint(serialized)
        asyncio.run(self._directory.refresh())

    def persist_sync(self) -> None:
        """
        Persists the current blueprint to S3.
        """
        assert self._blueprint is not None
        serialized = serialize_blueprint(self._blueprint)
        self._assets.persist_sync(
            _METADATA_KEY_TEMPLATE.format(self._schema_name), serialized
        )

    def delete_sync(self) -> None:
        """
        Deletes the persisted blueprint from S3.
        """
        self._assets.delete_sync(_METADATA_KEY_TEMPLATE.format(self._schema_name))

    @property
    def schema_name(self) -> str:
        return self._schema_name

    def get_blueprint(self) -> Blueprint:
        assert self._blueprint is not None
        return self._blueprint

    def set_blueprint(self, blueprint: Blueprint) -> None:
        self._blueprint = blueprint

    def get_directory(self) -> Directory:
        return self._directory


_METADATA_KEY_TEMPLATE = "{}.brad"
