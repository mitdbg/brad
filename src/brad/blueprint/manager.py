import asyncio
import json
import logging
from typing import Optional, Tuple

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.serde import (
    serialize_blueprint,
    deserialize_blueprint,
)
from brad.blueprint.state import TransitionState
from brad.config.file import ConfigFile
from brad.planner.scoring.score import Score
from brad.provisioning.directory import Directory

logger = logging.getLogger(__name__)


class BlueprintManager:
    """
    Utility class used for loading and providing access to the current blueprint.
    """

    def __init__(
        self,
        config: ConfigFile,
        assets: AssetManager,
        schema_name: str,
        initial_directory: Optional[Directory] = None,
    ) -> None:
        self._assets = assets
        self._schema_name = schema_name
        self._versioning: Optional[BlueprintVersioning] = None
        self._current_blueprint: Optional[Blueprint] = None
        self._current_blueprint_score: Optional[Score] = None
        self._next_blueprint: Optional[Blueprint] = None
        self._next_blueprint_score: Optional[Score] = None
        self._directory = (
            Directory(config) if initial_directory is None else initial_directory
        )
        self._config = config

    @staticmethod
    def initialize_schema(
        assets: AssetManager,
        blueprint: Blueprint,
    ) -> None:
        """
        Used when bootstrapping a new schema.
        """

        serialized = serialize_blueprint(blueprint)
        versioning = BlueprintVersioning(0, TransitionState.Stable, None)
        assets.persist_sync(
            BlueprintManager._blueprint_key_for_version(
                blueprint.schema_name(), version=0
            ),
            serialized,
        )
        assets.persist_sync(
            _VERSION_KEY.format(schema_name=blueprint.schema_name()),
            versioning.serialize(),
        )

    async def load(self, skip_directory_refresh: bool = False) -> None:
        """
        Loads the persisted version of the blueprint from S3.
        """
        try:
            versioning = await self._load_versioning()
        except ValueError:
            self._upgrade_legacy_format()
            versioning = await self._load_versioning()
            # If there is still a problem, we want the exception to stop BRAD.

        current_blueprint = await self._load_blueprint_version(versioning.version)
        current_score = await self._load_score(versioning.version)

        if versioning.next_version is not None:
            next_blueprint = await self._load_blueprint_version(versioning.next_version)
            next_score = await self._load_score(versioning.next_version)
        else:
            next_blueprint = None
            next_score = None

        # We want these values to be set together (i.e., not across `await`
        # boundaries).
        self._versioning = versioning
        self._current_blueprint = current_blueprint
        self._current_blueprint_score = current_score
        self._next_blueprint = next_blueprint
        self._next_blueprint_score = next_score

        if not skip_directory_refresh:
            # Ensure the Redshift cluster ID is overridden.
            if self._config.use_preset_redshift_clusters:
                preset_redshift_cluster_id = (
                    self._config.get_preset_redshift_cluster_id(
                        self._current_blueprint.redshift_provisioning()
                    )
                )
                self._directory.set_override_redshift_cluster_id(
                    preset_redshift_cluster_id
                )
            await self._directory.refresh()
        logger.debug("Loaded %s", self._versioning)

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
        self._current_blueprint_score = self._load_score_sync(self._versioning.version)
        if self._versioning.next_version is not None:
            self._next_blueprint = self._load_blueprint_version_sync(
                self._versioning.next_version
            )
            self._next_blueprint_score = self._load_score_sync(
                self._versioning.next_version
            )
        else:
            self._next_blueprint = None
            self._next_blueprint_score = None
        asyncio.run(self._directory.refresh())
        logger.debug("Loaded %s", self._versioning)

    async def fetch_historical_version(
        self, version: int
    ) -> Tuple[Blueprint, Optional[Score]]:
        """
        Used for administrative purposes only.
        """
        blueprint = await self._load_blueprint_version(version)
        score = await self._load_score(version)
        return blueprint, score

    async def start_transition(
        self, new_blueprint: Blueprint, new_score: Optional[Score]
    ) -> int:
        assert self._versioning is not None, "Run load() first."
        assert (
            self._versioning.transition_state == TransitionState.Stable
        ), "New blueprint is not yet stable."

        next_version = self._versioning.version + 1
        serialized = serialize_blueprint(new_blueprint)
        await self._assets.persist(
            self._blueprint_key_for_version(self._schema_name, next_version),
            serialized,
        )
        if new_score is not None:
            serialized_score = new_score.serialize()
            await self._assets.persist(
                self._score_key_for_version(next_version), serialized_score
            )

        next_versioning = self._versioning.copy()
        next_versioning.next_version = next_version
        next_versioning.transition_state = TransitionState.Transitioning
        await self._assets.persist(
            _VERSION_KEY.format(schema_name=self._schema_name),
            next_versioning.serialize(),
        )
        self._versioning = next_versioning
        self._next_blueprint = new_blueprint
        self._next_blueprint_score = new_score
        return next_version

    async def update_transition_state(self, next_state: TransitionState) -> None:
        assert self._versioning is not None, "Run load() first."
        assert (
            self._versioning.transition_state is not TransitionState.Stable
        ), "Can only update transition state during a transition."

        next_versioning = self._versioning.copy()
        next_versioning.transition_state = next_state
        if next_state == TransitionState.Stable:
            assert next_versioning.next_version is not None
            next_versioning.version = next_versioning.next_version
            next_versioning.next_version = None
        await self._assets.persist(
            _VERSION_KEY.format(schema_name=self._schema_name),
            next_versioning.serialize(),
        )
        self._versioning = next_versioning

        if next_state == TransitionState.Stable:
            self._current_blueprint = self._next_blueprint
            self._current_blueprint_score = self._next_blueprint_score
            self._next_blueprint = None
            self._next_blueprint_score = None

    async def dangerously_abort_transition(self) -> None:
        """
        Used for debugging purposes only; only call if you know what you are
        doing!
        """
        assert self._versioning is not None, "Run load() first."
        assert (
            self._versioning.transition_state is not TransitionState.Stable
        ), "Can only update transition state during a transition."

        next_versioning = self._versioning.copy()
        next_versioning.transition_state = TransitionState.Stable
        next_versioning.version = self._versioning.version
        next_versioning.next_version = None
        await self._assets.persist(
            _VERSION_KEY.format(schema_name=self._schema_name),
            next_versioning.serialize(),
        )
        self._versioning = next_versioning
        self._next_blueprint = None
        self._next_blueprint_score = None

    def force_new_blueprint_sync(
        self, blueprint: Blueprint, score: Optional[Score]
    ) -> None:
        """
        This is used when making manual changes to the blueprint. The provided
        blueprint will be persisted as the next stable version.
        """
        assert self._versioning is not None
        serialized = serialize_blueprint(blueprint)
        versioning = self._versioning.copy()
        versioning.version += 1
        versioning.transition_state = TransitionState.Stable
        versioning.next_version = None
        self._assets.persist_sync(
            self._blueprint_key_for_version(self._schema_name, versioning.version),
            serialized,
        )
        if score is not None:
            self._assets.persist_sync(
                self._score_key_for_version(versioning.version), score.serialize()
            )
        self._assets.persist_sync(
            _VERSION_KEY.format(schema_name=self._schema_name), versioning.serialize()
        )
        self._versioning = versioning
        self._current_blueprint = blueprint
        self._next_blueprint = None

    def delete_sync(self) -> None:
        """
        Logically deletes this entire schema.
        """
        self._assets.delete_sync(
            _LEGACY_METADATA_KEY_TEMPLATE.format(self._schema_name)
        )
        self._assets.delete_sync(_VERSION_KEY.format(schema_name=self._schema_name))

    @property
    def schema_name(self) -> str:
        return self._schema_name

    def get_blueprint(self) -> Blueprint:
        """
        This is used to retrieve the **active** blueprint.
        """
        assert self._versioning is not None
        if (
            self._versioning.transition_state is not TransitionState.CleaningUp
            and self._versioning.transition_state
            is not TransitionState.TransitionedPreCleanUp
        ):
            assert self._current_blueprint is not None
            return self._current_blueprint
        else:
            assert self._next_blueprint is not None
            return self._next_blueprint

    def get_active_blueprint_version(self) -> int:
        """
        This is used to retrieve the version associated with the **active**
        blueprint.
        """
        assert self._versioning is not None
        if (
            self._versioning.transition_state is not TransitionState.CleaningUp
            and self._versioning.transition_state
            is not TransitionState.TransitionedPreCleanUp
        ):
            return self._versioning.version
        else:
            assert self._versioning.next_version is not None
            return self._versioning.next_version

    def get_active_score(self) -> Optional[Score]:
        """
        This is used to retrieve the score associated with the **active**
        blueprint.
        """
        assert self._versioning is not None
        if (
            self._versioning.transition_state is not TransitionState.CleaningUp
            and self._versioning.transition_state
            is not TransitionState.TransitionedPreCleanUp
        ):
            return self._current_blueprint_score
        else:
            assert self._versioning.next_version is not None
            return self._next_blueprint_score

    def get_transition_metadata(self) -> "TransitionMetadata":
        assert self._versioning is not None
        assert self._current_blueprint is not None
        # Store versioned blueprints here too.
        return TransitionMetadata(
            state=self._versioning.transition_state,
            curr_version=self._versioning.version,
            curr_blueprint=self._current_blueprint,
            next_version=self._versioning.next_version,
            next_blueprint=self._next_blueprint,
        )

    def get_directory(self) -> Directory:
        return self._directory

    async def refresh_directory(self) -> None:
        await self._directory.refresh()

    async def _load_versioning(self) -> "BlueprintVersioning":
        version_data = await self._assets.load(
            _VERSION_KEY.format(schema_name=self._schema_name)
        )
        return BlueprintVersioning.deserialize(version_data)

    async def _load_blueprint_version(self, version: int) -> Blueprint:
        serialized = await self._assets.load(
            self._blueprint_key_for_version(self._schema_name, version)
        )
        return deserialize_blueprint(serialized)

    def _load_versioning_sync(self) -> "BlueprintVersioning":
        version_data = self._assets.load_sync(
            _VERSION_KEY.format(schema_name=self._schema_name)
        )
        return BlueprintVersioning.deserialize(version_data)

    def _load_blueprint_version_sync(self, version: int) -> Blueprint:
        serialized = self._assets.load_sync(
            self._blueprint_key_for_version(self._schema_name, version)
        )
        return deserialize_blueprint(serialized)

    async def _load_score(self, version: int) -> Optional[Score]:
        try:
            serialized = await self._assets.load(self._score_key_for_version(version))
            return Score.deserialize(serialized)
        except ValueError:
            return None

    def _load_score_sync(self, version: int) -> Optional[Score]:
        try:
            serialized = self._assets.load_sync(self._score_key_for_version(version))
            return Score.deserialize(serialized)
        except ValueError:
            return None

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
                "Failed to load the blueprint. Check if you have bootstrapped this schema."
            ) from ex

        versioning = BlueprintVersioning(0, TransitionState.Stable, None)
        self._assets.persist_sync(
            self._blueprint_key_for_version(self._schema_name, version=0),
            serialized,
        )
        self._assets.persist_sync(
            _VERSION_KEY.format(schema_name=self._schema_name), versioning.serialize()
        )
        logger.info("Completed upgrading the persisted blueprint format.")

        # NOTE: We do not delete the existing blueprint.

    @staticmethod
    def _blueprint_key_for_version(schema_name: str, version: int) -> str:
        return _METADATA_KEY_TEMPLATE.format(
            schema_name=schema_name, version=str(version).zfill(5)
        )

    def _score_key_for_version(self, version: int) -> str:
        return _SCORE_KEY_TEMPLATE.format(
            schema_name=self._schema_name, version=str(version).zfill(5)
        )


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

    def __repr__(self) -> str:
        return "".join(
            [
                "BlueprintVersioning(version=",
                str(self.version),
                ", transition_state=",
                str(self.transition_state),
                ", next_version=",
                str(self.next_version),
                ")",
            ]
        )

    def copy(self) -> "BlueprintVersioning":
        return BlueprintVersioning(
            self.version, self.transition_state, self.next_version
        )


class TransitionMetadata:
    """
    Holds information about a blueprint transition.
    """

    def __init__(
        self,
        state: TransitionState,
        curr_version: int,
        curr_blueprint: Blueprint,
        next_version: Optional[int],
        next_blueprint: Optional[Blueprint],
    ) -> None:
        self.state = state
        self.curr_version = curr_version
        self.curr_blueprint = curr_blueprint
        self.next_version = next_version
        self.next_blueprint = next_blueprint


_LEGACY_METADATA_KEY_TEMPLATE = "{}.brad"

_VERSION_KEY = "{schema_name}/blueprints/BRAD"
_METADATA_KEY_TEMPLATE = "{schema_name}/blueprints/BRAD-BP-{version}"
_SCORE_KEY_TEMPLATE = "{schema_name}/scores/BRAD-SCORE-{version}.pickle"
