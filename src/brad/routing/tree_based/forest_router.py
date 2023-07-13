import asyncio
from typing import Optional, Dict

from .model_wrap import ModelWrap
from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.config.engine import Engine, EngineBitmapValues
from brad.data_stats.estimator import Estimator
from brad.query_rep import QueryRep
from brad.routing.policy import RoutingPolicy
from brad.routing.router import Router
from brad.server.blueprint_manager import BlueprintManager


class ForestRouter(Router):
    @classmethod
    def for_server(
        cls,
        policy: RoutingPolicy,
        schema_name: str,
        assets: AssetManager,
        blueprint_mgr: BlueprintManager,
    ) -> "ForestRouter":
        return cls(policy, schema_name, assets=assets, blueprint_mgr=blueprint_mgr)

    @classmethod
    def for_planner(
        cls,
        policy: RoutingPolicy,
        schema_name: str,
        model: ModelWrap,
        table_bitmap: Dict[str, int],
    ) -> "ForestRouter":
        return cls(
            policy, schema_name, model=model, table_placement_bitmap=table_bitmap
        )

    def __init__(
        self,
        policy: RoutingPolicy,
        schema_name: str,
        # One of `assets` and `model` most not be `None`.
        assets: Optional[AssetManager] = None,
        model: Optional[ModelWrap] = None,
        # One of `blueprint_mgr`, `blueprint`, and `table_placement_bitmap` must not be `None`.
        blueprint_mgr: Optional[BlueprintManager] = None,
        blueprint: Optional[Blueprint] = None,
        table_placement_bitmap: Optional[Dict[str, int]] = None,
    ) -> None:
        self._policy = policy
        self._schema_name = schema_name
        self._model = model
        self._assets = assets

        self._blueprint_mgr = blueprint_mgr
        self._blueprint = blueprint
        self._table_placement_bitmap = table_placement_bitmap
        self._estimator: Optional[Estimator] = None

    async def run_setup(self, estimator: Optional[Estimator] = None) -> None:
        self._estimator = estimator

        # Load the model.
        if self._model is None:
            assert self._assets is not None
            serialized_model = await self._assets.load(
                _SERIALIZED_KEY.format(
                    schema_name=self._schema_name, policy=self._policy.value
                )
            )
            self._model = ModelWrap.from_pickle_bytes(serialized_model)

        # Load the table placement if it was not provided.
        if self._table_placement_bitmap is None:
            if self._blueprint is None:
                assert self._blueprint_mgr is not None
                self._blueprint = self._blueprint_mgr.get_blueprint()

            self._table_placement_bitmap = self._blueprint.table_locations_bitmap()

    async def engine_for(self, query: QueryRep) -> Engine:
        # Compute valid locations.
        assert self._table_placement_bitmap is not None
        valid_locations, only_location = self._run_location_routing(
            query, self._table_placement_bitmap
        )
        if only_location is not None:
            return only_location

        # Multiple locations possible. Use the model to figure out which location to use.
        assert self._model is not None
        preferred_locations = await self._model.engine_for(query, self._estimator)

        for loc in preferred_locations:
            if (EngineBitmapValues[loc] & valid_locations) != 0:
                return loc

        # This should be unreachable. The model must rank all engines, and we
        # know >= 2 engines can support this query.
        raise AssertionError

    def engine_for_sync(self, query: QueryRep) -> Engine:
        return asyncio.run(self.engine_for(query))

    def persist_sync(self) -> None:
        assert self._assets is not None
        assert self._model is not None
        self.static_persist_sync(self._model, self._schema_name, self._assets)

    @staticmethod
    def static_persist_sync(
        model: ModelWrap, schema_name: str, assets: AssetManager
    ) -> None:
        key = _SERIALIZED_KEY.format(
            schema_name=schema_name, policy=model.policy().value
        )
        serialized = model.to_pickle()
        assets.persist_sync(key, serialized)

    @staticmethod
    def static_load_model_sync(
        schema_name: str, policy: RoutingPolicy, assets: AssetManager
    ) -> ModelWrap:
        key = _SERIALIZED_KEY.format(schema_name=schema_name, policy=policy.value)
        serialized = assets.load_sync(key)
        return ModelWrap.from_pickle_bytes(serialized)

    @staticmethod
    def static_drop_model_sync(
        schema_name: str, policy: RoutingPolicy, assets: AssetManager
    ) -> None:
        key = _SERIALIZED_KEY.format(schema_name=schema_name, policy=policy.value)
        assets.delete_sync(key)


_SERIALIZED_KEY = "{schema_name}/{policy}-router.pickle"
