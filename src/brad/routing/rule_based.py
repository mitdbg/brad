from typing import Set, List, Optional

from brad.config.engine import Engine
from brad.server.blueprint_manager import BlueprintManager
from brad.daemon.monitor import Monitor
from brad.routing import Router
from brad.query_rep import QueryRep


class RuleBased(Router):
    def __init__(
        self,
        blueprint_mgr: BlueprintManager,
        monitor: Optional[Monitor] = None,
        deterministic: bool = True,
    ):
        self._blueprint_mgr = blueprint_mgr
        self._monitor = monitor
        # deterministic routing guarantees the same decision for the same query and should be used online
        # non-determinism will be used for offline training data exploration (not implemented)
        self.deterministic = deterministic

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
        elif len(locations) == 1:
            return locations[0]
        else:
            ideal_location_rank: List[Engine] = []
            # Todo: include infos on table size (can be stored in this class and updated at each maintainence window)
            # Todo: add more rules (e.g. index, simple selective)
            if len(query.tables()) >= 6:
                ideal_location_rank = [Engine.Athena, Engine.Redshift, Engine.Aurora]
            elif len(query.tables()) >= 3:
                ideal_location_rank = [Engine.Redshift, Engine.Athena, Engine.Aurora]
            else:
                ideal_location_rank = [Engine.Aurora, Engine.Redshift, Engine.Athena]
            ideal_location_rank = [
                loc for loc in ideal_location_rank if loc in locations
            ]
            if self._monitor is None:
                return ideal_location_rank[0]
            else:
                sys_metric = self._monitor.read_k_most_recent(k=1)
                # Todo: understand sys_metric format and design rules to filter ideal_location_rank
                return ideal_location_rank[0]
