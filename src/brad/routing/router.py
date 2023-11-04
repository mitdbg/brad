import asyncio
import logging
from typing import Dict, Tuple, Optional, TYPE_CHECKING

from brad.data_stats.estimator import Estimator
from brad.config.engine import Engine, EngineBitmapValues
from brad.query_rep import QueryRep
from brad.routing.abstract_policy import AbstractRoutingPolicy, FullRoutingPolicy

if TYPE_CHECKING:
    from brad.blueprint import Blueprint

logger = logging.getLogger(__name__)


class Router:
    @classmethod
    def create_from_blueprint(cls, blueprint: "Blueprint") -> "Router":
        return cls(
            blueprint.get_routing_policy(),
            blueprint.table_locations_bitmap(),
            use_future_blueprint_policies=True,
        )

    @classmethod
    def create_from_definite_policy(
        cls, policy: AbstractRoutingPolicy, table_placement_bitmap: Dict[str, int]
    ) -> "Router":
        return cls(
            FullRoutingPolicy(indefinite_policies=[], definite_policy=policy),
            table_placement_bitmap,
            use_future_blueprint_policies=False,
        )

    def __init__(
        self,
        full_policy: FullRoutingPolicy,
        table_placement_bitmap: Dict[str, int],
        use_future_blueprint_policies: bool,
    ) -> None:
        self._full_policy = full_policy
        self._table_placement_bitmap = table_placement_bitmap
        self._use_future_blueprint_policies = use_future_blueprint_policies

    def log_policy(self) -> None:
        logger.info("Routing policy:")
        logger.info("  Indefinite policies:")
        for p in self._full_policy.indefinite_policies:
            logger.info("    - %s", p.name())
        logger.info("  Definite policy: %s")

    async def run_setup(self, estimator: Optional[Estimator] = None) -> None:
        """
        Should be called before using the router. This is used to set up any
        dynamic state.

        If the routing policy needs an estimator, one should be provided here.
        """
        await self._full_policy.run_setup(estimator)

    def update_blueprint(self, blueprint: "Blueprint") -> None:
        """
        Used to update any cached state that depends on the blueprint (e.g.,
        location bitmaps).
        """
        self._table_placement_bitmap = blueprint.table_locations_bitmap()
        if self._use_future_blueprint_policies:
            self._full_policy = blueprint.get_routing_policy()

    async def engine_for(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.
        """

        # Table placement constraints.
        assert self._table_placement_bitmap is not None
        valid_locations, only_location = self._run_location_routing(
            query, self._table_placement_bitmap
        )
        if only_location is not None:
            return only_location

        # Go through the indefinite routing policies. These may not return a
        # routing location.
        for policy in self._full_policy.indefinite_policies:
            locations = await policy.engine_for(query)
            for loc in locations:
                if (EngineBitmapValues[loc] & valid_locations) != 0:
                    return loc

        # Rely on the definite routing policy.
        locations = await self._full_policy.definite_policy.engine_for(query)
        for loc in locations:
            if (EngineBitmapValues[loc] & valid_locations) != 0:
                return loc

        # This should be unreachable. The definite policy must rank all engines,
        # and we know >= 2 engines can support this query.
        raise AssertionError

    def engine_for_sync(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.

        NOTE: Implementers currently do not need to consider DML queries. BRAD
        routes all DML queries to Aurora before consulting the router. Thus the
        query passed to this method will always be a read-only query.
        """
        # Ideally we re-implement a sync version.
        return asyncio.run(self.engine_for(query))

    def _run_location_routing(
        self, query: QueryRep, location_bitmap: Dict[str, int]
    ) -> Tuple[int, Optional[Engine]]:
        """
        Based on the provided location bitmap, compute the set of engines that
        are able to serve this query. If there is only one possible engine, this
        method will also return that engine.
        """

        # Narrow down the valid engines that can run the query, based on the
        # table placement.
        valid_locations = Engine.bitmap_all()
        for table_name_str in query.tables():
            try:
                valid_locations &= location_bitmap[table_name_str]
            except KeyError:
                # The query is referencing a non-existent table (could be a CTE
                # - the parser does not differentiate between CTE tables and
                # "actual" tables).
                pass

        if valid_locations == 0:
            # This happens when a query references a set of tables that do not
            # all have a presence in the same location.
            raise RuntimeError(
                "A single location is not available for tables {}".format(
                    ", ".join(query.tables())
                )
            )

        if (valid_locations & (valid_locations - 1)) == 0:
            # Bitmap trick - only one bit is set.
            if (EngineBitmapValues[Engine.Aurora] & valid_locations) != 0:
                return (valid_locations, Engine.Aurora)
            elif (EngineBitmapValues[Engine.Redshift] & valid_locations) != 0:
                return (valid_locations, Engine.Redshift)
            elif (EngineBitmapValues[Engine.Athena] & valid_locations) != 0:
                return (valid_locations, Engine.Athena)
            else:
                raise RuntimeError("Unsupported bitmap value " + str(valid_locations))

        # There is more than one possible location.
        return (valid_locations, None)
