import asyncio
import json
import logging
from typing import Optional

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.serde import (
    serialize_blueprint,
    deserialize_blueprint,
)
from brad.blueprint.state import TransitionState
from brad.config.file import ConfigFile
from brad.provisioning.directory import Directory

logger = logging.getLogger(__name__)


class BlueprintManager:
    """
    Utility class used for loading and providing access to the current blueprint.
    """

    def __init__(
        self, config: ConfigFile, assets: AssetManager, schema_name: str
    ) -> None:
        self._assets = assets
        self._schema_name = schema_name
        self._versioning: Optional[BlueprintVersioning] = None
        self._current_blueprint: Optional[Blueprint] = None
        self._next_blueprint: Optional[Blueprint] = None
        self._directory = Directory(config)

    async def load(self) -> None:
        """
        Loads the persisted version of the blueprint from S3.
        """
        try:
            self._versioning = await self._load_versioning()
        except ValueError:
            self._upgrade_legacy_format()
            self._versioning = await self._load_versioning()
            # If there is still a problem, we want the exception to stop BRAD.

        self._current_blueprint = await self._load_blueprint_version(
            self._versioning.version
        )
        if self._versioning.next_version is not None:
            self._next_blueprint = await self._load_blueprint_version(
                self._versioning.next_version
            )
        else:
            self._next_blueprint = None
        await self._directory.refresh()

    def load_sync(self) -> None:
        try:
            self._versioning = self._load_versioning_sync()
        except ValueError:
            self._upgrade_legacy_format()
            self._versioning = self._load_versioning_sync()
            # If there is still a problem, we want the exception to stop BRAD.

        self._current_blueprint = self._load_blueprint_version_sync(
            self._versioning.version
        )
        if self._versioning.next_version is not None:
            self._next_blueprint = self._load_blueprint_version_sync(
                self._versioning.next_version
            )
        else:
            self._next_blueprint = None
        asyncio.run(self._directory.refresh())

    async def persist(self) -> None:
        """
        Persists the current blueprint to S3.
        """
        assert self._blueprint is not None
        serialized = serialize_blueprint(self._blueprint)
        await self._assets.persist(
            _METADATA_KEY_TEMPLATE.format(self._schema_name), serialized
        )

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
        self._assets.delete_sync(_LEGACY_METADATA_KEY_TEMPLATE.format(self._schema_name))
        self._assets.delete_sync(_VERSION_KEY.format(self._schema_name))

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

    async def _load_versioning(self) -> "BlueprintVersioning":
        version_data = await self._assets.load(_VERSION_KEY.format(self._schema_name))
        return BlueprintVersioning.deserialize(version_data)

    async def _load_blueprint_version(self, version: int) -> Blueprint:
        serialized = await self._assets.load(
            _METADATA_KEY_TEMPLATE.format(
                schema_name=self._schema_name, version=version
            )
        )
        return deserialize_blueprint(serialized)

    def _load_versioning_sync(self) -> "BlueprintVersioning":
        version_data = self._assets.load_sync(_VERSION_KEY.format(self._schema_name))
        return BlueprintVersioning.deserialize(version_data)

    def _load_blueprint_version_sync(self, version: int) -> Blueprint:
        serialized = self._assets.load_sync(
            _METADATA_KEY_TEMPLATE.format(
                schema_name=self._schema_name, version=version
            )
        )
        return deserialize_blueprint(serialized)

    def _upgrade_legacy_format(self) -> None:
        logger.info(
            "Detected possible legacy blueprint format. Attempting an upgrade now..."
        )
        # This is used to handle existing blueprints in the old format (without
        # versioning information).
        try:
            serialized = self._assets.load_sync(
                _LEGACY_METADATA_KEY_TEMPLATE.format(self._schema_name)
            )
        except ValueError as ex:
            raise RuntimeError(
                "Failed to load the blueprint. Check if you have bootstraped this schema."
            ) from ex

        versioning = BlueprintVersioning(0, TransitionState.Stable, None)
        self._assets.persist_sync(
            _METADATA_KEY_TEMPLATE.format(schema_name=self._schema_name, version=0),
            serialized,
        )
        self._assets.persist_sync(_VERSION_KEY, versioning.serialize())
        logger.info("Completed upgrading the persisted blueprint format.")

        # NOTE: We do not delete the existing blueprint.


class BlueprintVersioning:
    def __init__(
        self,
        version: int,
        transition_state: TransitionState,
        next_version: Optional[int],
    ) -> None:
        self.version = version
        self.transition_state = transition_state
        self.next_version = next_version

    def serialize(self) -> bytes:
        parts = [self.version, self.transition_state.value, self.next_version]
        return json.dumps(parts).encode()

    @classmethod
    def deserialize(cls, data: bytes) -> "BlueprintVersioning":
        parts = json.loads(data.decode())
        return cls(
            int(parts[0]),
            TransitionState.from_str(parts[1]),
            int(parts[2]) if parts[2] is not None else None,
        )


_LEGACY_METADATA_KEY_TEMPLATE = "{}.brad"

_VERSION_KEY = "{schema_name}/blueprints/VERSION"
_METADATA_KEY_TEMPLATE = "{schema_name}/blueprints/bp_{version}"
