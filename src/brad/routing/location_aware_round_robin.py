from brad.config.engine import Engine
from brad.blueprint_manager import BlueprintManager
from brad.routing.router import Router
from brad.query_rep import QueryRep


class LocationAwareRoundRobin(Router):
    """
    Routes queries in a "roughly" round-robin fashion, taking into account the
    locations of the tables referenced.
    """

    def __init__(self, blueprint_mgr: BlueprintManager):
        self._blueprint_mgr = blueprint_mgr
        self._curr_idx = 0

    def engine_for_sync(self, query: QueryRep) -> Engine:
        blueprint = self._blueprint_mgr.get_blueprint()
        valid_locations, only_location = self._run_location_routing(
            query, blueprint.table_locations_bitmap()
        )
        if only_location is not None:
            return only_location

        locations = Engine.from_bitmap(valid_locations)
        self._curr_idx %= len(locations)
        selected_location = locations[self._curr_idx]
        self._curr_idx += 1

        return selected_location
