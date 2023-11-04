from typing import Dict, Optional

from brad.asset_manager import AssetManager
from brad.config.file import ConfigFile
from brad.planner.estimator import EstimatorProvider
from brad.routing.abstract_policy import AbstractRoutingPolicy
from brad.routing.policy import RoutingPolicy
from brad.routing.router import Router
from brad.routing.rule_based import RuleBased
from brad.routing.tree_based.forest_policy import ForestPolicy
from brad.routing.tree_based.model_wrap import ModelWrap


class RouterProvider:
    """
    A temporary helper class used for injecting routers into the blueprint
    planner. This is a workaround until we put the routing policy into the
    blueprint itself.
    """

    def __init__(
        self,
        schema_name: str,
        config: ConfigFile,
        estimator_provider: EstimatorProvider,
    ) -> None:
        self._schema_name = schema_name
        self._config = config
        self._assets = AssetManager(self._config)
        self._routing_policy = self._config.routing_policy
        self._estimator_provider = estimator_provider

        # We cache this model to avoid loading it from S3 repeatedly.
        self._model: Optional[ModelWrap] = None

    async def get_router(self, table_bitmap: Dict[str, int]) -> Router:
        if (
            self._routing_policy == RoutingPolicy.ForestTablePresence
            or self._routing_policy == RoutingPolicy.ForestTableSelectivity
        ):
            if self._model is None:
                self._model = ForestPolicy.static_load_model_sync(
                    self._schema_name,
                    self._routing_policy,
                    self._assets,
                )
            definite_policy: AbstractRoutingPolicy = ForestPolicy.from_loaded_model(
                self._routing_policy, self._model
            )

        elif self._routing_policy == RoutingPolicy.RuleBased:
            definite_policy = RuleBased(table_placement_bitmap=table_bitmap)

        else:
            raise RuntimeError(
                "BlueprintPlanner unsupported routing policy: {}".format(
                    self._routing_policy.value
                )
            )

        # This is temporary and will be removed.
        router = Router.create_from_definite_policy(definite_policy, table_bitmap)
        await router.run_setup(self._estimator_provider.get_estimator())
        return router

    def clear_cached(self) -> None:
        self._model = None
