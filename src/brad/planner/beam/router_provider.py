from typing import Dict, Optional

from brad.asset_manager import AssetManager
from brad.config.file import ConfigFile
from brad.routing.policy import RoutingPolicy
from brad.routing.router import Router
from brad.routing.rule_based import RuleBased
from brad.routing.tree_based.forest_router import ForestRouter
from brad.routing.tree_based.model_wrap import ModelWrap


class RouterProvider:
    """
    A temporary helper class used for injecting routers into the blueprint
    planner. This is a workaround until we put the routing policy into the
    blueprint itself.
    """

    def __init__(self, schema_name: str, config: ConfigFile) -> None:
        self._schema_name = schema_name
        self._config = config
        self._assets = AssetManager(self._config)
        self._routing_policy = self._config.routing_policy

        # We cache this model to avoid loading it from S3 repeatedly.
        self._model: Optional[ModelWrap] = None

    def get_router(self, table_bitmap: Dict[str, int]) -> Router:
        if (
            self._routing_policy == RoutingPolicy.ForestTablePresence
            or self._routing_policy == RoutingPolicy.ForestTableSelectivity
        ):
            if self._model is None:
                self._model = ForestRouter.static_load_model_sync(
                    self._schema_name,
                    self._routing_policy,
                    self._assets,
                )
            return ForestRouter.for_planner(
                self._routing_policy, self._schema_name, self._model, table_bitmap
            )

        elif self._routing_policy == RoutingPolicy.RuleBased:
            return RuleBased(table_placement_bitmap=table_bitmap)

        else:
            raise RuntimeError(
                "BlueprintPlanner unsupported routing policy: {}".format(
                    self._routing_policy.value
                )
            )

    def clear_cached(self) -> None:
        self._model = None
