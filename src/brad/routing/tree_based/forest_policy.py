import asyncio
from typing import Optional, List, Dict, Any

from brad.asset_manager import AssetManager
from brad.config.engine import Engine
from brad.data_stats.estimator import Estimator
from brad.query_rep import QueryRep
from brad.routing.abstract_policy import AbstractRoutingPolicy
from brad.routing.policy import RoutingPolicy
from brad.routing.tree_based.model_wrap import ModelWrap


class ForestPolicy(AbstractRoutingPolicy):
    @classmethod
    async def from_assets(
        cls, schema_name: str, policy: RoutingPolicy, assets: AssetManager
    ) -> "ForestPolicy":
        model = cls.static_load_model_sync(schema_name, policy, assets)
        return cls(policy, model)

    @classmethod
    def from_loaded_model(
        cls, policy: RoutingPolicy, model: ModelWrap
    ) -> "ForestPolicy":
        return cls(policy, model)

    def __init__(self, policy: RoutingPolicy, model: ModelWrap) -> None:
        self._policy = policy
        self._model = model
        self._estimator: Optional[Estimator] = None

    def __getstate__(self) -> Dict[Any, Any]:
        return {
            "policy": self._policy,
            "model": self._model,
        }

    def __setstate__(self, d: Dict[Any, Any]) -> None:
        self._policy = d["policy"]
        self._model = d["model"]
        self._estimator = None

    def name(self) -> str:
        return f"ForestPolicy({self._policy.name})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ForestPolicy):
            return False
        return self._policy == other._policy and self._model == other.model

    async def run_setup(self, estimator: Optional[Estimator] = None) -> None:
        self._estimator = estimator

    async def engine_for(self, query_rep: QueryRep) -> List[Engine]:
        return await self._model.engine_for(query_rep, self._estimator)

    def engine_for_sync(self, query_rep: QueryRep) -> List[Engine]:
        return asyncio.run(self.engine_for(query_rep))

    # The methods below are used to save/load `ModelWrap` from S3. We
    # historically separated out the model's implementation details because the
    # router contained state that was not serializable. This separation is kept
    # around for legacy reasons now (this policy class should be directly
    # serialized).

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
