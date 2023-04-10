from typing import Set, List

from brad.config.engine import Engine
from brad.server.blueprint_manager import BlueprintManager
from brad.routing import Router
from brad.query_rep import QueryRep


class LocationAwareRoundRobin(Router):
    """
    Routes queries in a "roughly" round-robin fashion, taking into account the
    locations of the tables referenced.
    """

    def __init__(self, blueprint_mgr: BlueprintManager):
        self._blueprint_mgr = blueprint_mgr
        self._curr_idx = 0

    def engine_for(self, query: QueryRep) -> Engine:
        if query.is_data_modification_query():
            return Engine.Aurora

        blueprint = self._blueprint_mgr.get_blueprint()

        location_sets: List[Set[Engine]] = []
        for table_name_str in query.tables():
            try:
                table = blueprint.get_table(table_name_str)
                location_sets.append(set(table.locations))
            except ValueError:
                # The query is referencing a non-existent table (could be a CTE
                # - the parser does not differentiate between CTE tables and
                # "actual" tables).
                pass
        locations: List[Engine] = (
            list(set.intersection(*location_sets)) if len(location_sets) > 0 else []
        )

        if len(locations) == 0:
            # This happens when a query references a set of tables that do not
            # all have a presence in the same location.
            raise RuntimeError(
                "A single location is not available for tables {}".format(
                    ", ".join(query.tables())
                )
            )

        self._curr_idx %= len(locations)
        selected_location = locations[self._curr_idx]
        self._curr_idx += 1

        return selected_location
