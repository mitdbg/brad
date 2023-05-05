from typing import List, Optional, Mapping

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.server.blueprint_manager import BlueprintManager
from brad.daemon.monitor import Monitor
from brad.routing import Router
from brad.query_rep import QueryRep


class RuleBasedParams(object):
    def __init__(self):
        # Aurora metric threshold
        aurora_parameters_upper_limit: Mapping[str, Optional[float]] = dict()
        aurora_parameters_upper_limit["aurora_WRITER_CPUUtilization_Average"] = 50.0
        aurora_parameters_upper_limit["aurora_WRITER_ReadIOPS_Average"] = None
        aurora_parameters_upper_limit["aurora_READER_CPUUtilization_Average"] = 50.0
        aurora_parameters_upper_limit["aurora_READER_ReadIOPS_Average"] = None
        aurora_parameters_upper_limit["aurora_READER_ReadLatency_Average"] = 1e-4
        self.aurora_parameters_upper_limit = aurora_parameters_upper_limit
        aurora_parameters_lower_limit: Mapping[str, Optional[float]] = dict()
        self.aurora_parameters_lower_limit = aurora_parameters_lower_limit

        # Redshift metric threshold
        redshift_parameters_upper_limit: Mapping[str, Optional[float]] = dict()
        redshift_parameters_upper_limit["redshift_CPUUtilization_Average"] = 50.0
        redshift_parameters_upper_limit["redshift_ReadIOPS_Average"] = None
        redshift_parameters_upper_limit["redshift_ReadLatency_Average"] = 1e-4
        self.redshift_parameters_upper_limit = redshift_parameters_upper_limit
        redshift_parameters_lower_limit: Mapping[str, Optional[float]] = dict()
        self.redshift_parameters_lower_limit = redshift_parameters_lower_limit


class RuleBased(Router):
    def __init__(
        self,
        # One of `blueprint_mgr` and `blueprint` must not be `None`.
        blueprint_mgr: Optional[BlueprintManager] = None,
        blueprint: Optional[Blueprint] = None,
        monitor: Optional[Monitor] = None,
        deterministic: bool = True,
    ):
        self._blueprint_mgr = blueprint_mgr
        self._blueprint = blueprint
        self._monitor = monitor
        # deterministic routing guarantees the same decision for the same query and should be used online
        # non-determinism will be used for offline training data exploration (not implemented)
        self.deterministic = deterministic
        self.params = RuleBasedParams()

    def check_engine_state(
        self, engine: Engine, sys_metric: Mapping[str, float]
    ) -> bool:
        # We define a set of rules to judge whether the current system is overloaded
        not_overloaded = True
        if engine == Engine.Aurora:
            for param in self.params.aurora_parameters_upper_limit:
                limit = self.params.aurora_parameters_upper_limit[param]
                if limit and param in sys_metric and sys_metric[param] > limit:
                    not_overloaded = False
                    break
            if not_overloaded:
                for param in self.params.aurora_parameters_lower_limit:
                    limit = self.params.aurora_parameters_lower_limit[param]
                    if limit and param in sys_metric and sys_metric[param] < limit:
                        not_overloaded = False
                        break

        if engine == Engine.Redshift:
            for param in self.params.redshift_parameters_upper_limit:
                limit = self.params.redshift_parameters_upper_limit[param]
                if limit and param in sys_metric and sys_metric[param] > limit:
                    not_overloaded = False
                    break
            if not_overloaded:
                for param in self.params.redshift_parameters_lower_limit:
                    limit = self.params.redshift_parameters_lower_limit[param]
                    if limit and param in sys_metric and sys_metric[param] < limit:
                        not_overloaded = False
                        break

        if engine == Engine.Athena:
            # Athena should always be not overloaded
            return True
        return not_overloaded

    def engine_for(self, query: QueryRep) -> Engine:
        if query.is_data_modification_query():
            return Engine.Aurora

        if self._blueprint is not None:
            blueprint = self._blueprint
        else:
            assert self._blueprint_mgr is not None
            blueprint = self._blueprint_mgr.get_blueprint()

        locations_bitmaps = blueprint.table_locations_bitmap()
        valid_locations = Engine.bitmap_all()
        for table_name_str in query.tables():
            try:
                valid_locations &= locations_bitmaps[table_name_str]
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

        locations = Engine.from_bitmap(valid_locations)
        if len(locations) == 1:
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

            if self._monitor is None:
                for loc in ideal_location_rank:
                    if loc in locations:
                        return loc
                # This should be unreachable since len(locations) > 0.
                assert False
            else:
                # Todo: this can be stored in this class to reduce latency
                raw_sys_metric = self._monitor.read_k_most_recent(k=1)
                col_name = list(raw_sys_metric.columns)
                col_value = list(raw_sys_metric.values)[0]
                sys_metric = {col_name[i]: col_value[i] for i in range(len(col_value))}
                for loc in ideal_location_rank:
                    if self.check_engine_state(loc, sys_metric):
                        return loc

                # In the case of all system are overloaded (time to trigger replan),
                # we assign it to the optimal one. But Athena should not be overloaded at any time
                return ideal_location_rank[0]
