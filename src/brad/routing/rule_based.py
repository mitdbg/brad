import os.path
import json
import logging
from typing import List, Optional, Mapping, MutableMapping, Any, Dict
from importlib.resources import files, as_file

import brad.routing
from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.blueprint_manager import BlueprintManager
from brad.daemon.monitor import Monitor
from brad.routing.router import Router
from brad.query_rep import QueryRep
from brad.front_end.session import SessionManager, Session

logger = logging.getLogger(__name__)


class RuleBasedParams(object):
    def __init__(self) -> None:
        # Ideal location threshold
        ideal_location_upper_limit: MutableMapping[str, float] = dict()
        ideal_location_upper_limit["aurora_num_table"] = 6
        ideal_location_upper_limit["aurora_table_size_max"] = 5e7
        ideal_location_upper_limit["aurora_table_size_sum"] = 2e8
        ideal_location_upper_limit["aurora_table_ncolumn_max"] = 15
        ideal_location_upper_limit["aurora_table_ncolumn_sum"] = 40
        ideal_location_upper_limit["redshift_num_table"] = 10
        ideal_location_upper_limit["redshift_table_size_max"] = 5e9
        ideal_location_upper_limit["redshift_table_size_sum"] = 2e11
        self.ideal_location_upper_limit = ideal_location_upper_limit
        ideal_location_lower_limit: MutableMapping[str, float] = dict()
        ideal_location_lower_limit["redshift_num_table"] = 3.0
        self.ideal_location_lower_limit = ideal_location_lower_limit

        # Aurora metric threshold
        aurora_parameters_upper_limit: MutableMapping[str, Optional[float]] = dict()
        aurora_parameters_upper_limit["aurora_WRITER_CPUUtilization_Average"] = 50.0
        aurora_parameters_upper_limit["aurora_WRITER_ReadIOPS_Average"] = None
        aurora_parameters_upper_limit["aurora_READER_CPUUtilization_Average"] = 50.0
        aurora_parameters_upper_limit["aurora_READER_ReadIOPS_Average"] = None
        aurora_parameters_upper_limit["aurora_READER_ReadLatency_Average"] = 1e-4
        self.aurora_parameters_upper_limit = aurora_parameters_upper_limit
        aurora_parameters_lower_limit: MutableMapping[str, Optional[float]] = dict()
        self.aurora_parameters_lower_limit = aurora_parameters_lower_limit

        # Redshift metric threshold
        redshift_parameters_upper_limit: MutableMapping[str, Optional[float]] = dict()
        redshift_parameters_upper_limit["redshift_CPUUtilization_Average"] = 50.0
        redshift_parameters_upper_limit["redshift_ReadIOPS_Average"] = None
        redshift_parameters_upper_limit["redshift_ReadLatency_Average"] = 1e-4
        self.redshift_parameters_upper_limit = redshift_parameters_upper_limit
        redshift_parameters_lower_limit: MutableMapping[str, Optional[float]] = dict()
        self.redshift_parameters_lower_limit = redshift_parameters_lower_limit


class RuleBased(Router):
    def __init__(
        self,
        # One of `blueprint_mgr` and `blueprint` must not be `None`.
        blueprint_mgr: Optional[BlueprintManager] = None,
        blueprint: Optional[Blueprint] = None,
        table_placement_bitmap: Optional[Dict[str, int]] = None,
        monitor: Optional[Monitor] = None,
        catalog: Optional[MutableMapping[str, MutableMapping[str, Any]]] = None,
        use_decision_tree: bool = False,
        deterministic: bool = True,
    ):
        self._blueprint_mgr = blueprint_mgr
        self._blueprint = blueprint
        self._table_placement_bitmap = table_placement_bitmap
        self._monitor = monitor
        # catalog contains all tables' number of rows and columns
        self._catalog = catalog
        if self._catalog is None:
            _catalog_file = files(brad.routing).joinpath("imdb_catalog.json")
            with as_file(_catalog_file) as file:
                if os.path.exists(file):
                    with open(file, "r", encoding="utf8") as f:
                        self._catalog = json.load(f)
        # use decision tree instead of rules
        self._use_decision_tree = use_decision_tree
        # deterministic routing guarantees the same decision for the same query and should be used online
        # non-determinism will be used for offline training data exploration (not implemented)
        self._deterministic = deterministic
        self._params = RuleBasedParams()

    async def recollect_catalog(self, sessions: SessionManager) -> None:
        # recollect catalog stats; happens every maintenance window
        if self._catalog is None:
            self._catalog = dict()
        session_id, _ = await sessions.create_new_session()
        session = sessions.get_session(session_id)
        assert (
            session is not None
        ), "need to provide a valid aurora session to recollect_catalog"
        # Since only Aurora handles txn, we only need connection to Aurora
        connection = session.engines.get_connection(Engine.Aurora)
        cursor = await connection.cursor()

        if self._blueprint is not None:
            blueprint = self._blueprint
        else:
            assert self._blueprint_mgr is not None
            blueprint = self._blueprint_mgr.get_blueprint()

        indexes_sql = (
            "SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' "
            "ORDER BY tablename, indexname;"
        )
        await cursor.execute(indexes_sql)
        all_indexes_raw = await cursor.fetchall()
        all_indexes: MutableMapping[str, List[List[str]]] = dict()
        for index in all_indexes_raw:
            brad_table_name = index[0]
            if brad_table_name not in all_indexes:
                all_indexes[brad_table_name] = []
            all_indexes[brad_table_name].append(list(index))

        for table_name in blueprint.table_locations():
            location = blueprint.table_locations()[table_name]
            if Engine.Aurora in location:
                # the following syntax only works for Aurora, we also assume all txn happens in Aurora
                # so if a table is not on Aurora, it will have no change.
                nrow_sql = f"SELECT COUNT(*) FROM {table_name};"
                await cursor.execute(nrow_sql)
                nrow = await cursor.fetchone()
                assert nrow is not None
                ncol_sql = f"""SELECT COUNT(*)
                                  FROM INFORMATION_SCHEMA.COLUMNS
                                  WHERE table_catalog = '{blueprint.schema_name()}'
                                  AND table_name = '{table_name}';
                           """
                await cursor.execute(ncol_sql)
                ncol = await cursor.fetchone()
                assert ncol is not None

                brad_table_name = table_name + "_brad_source"
                table_indexes = []
                table_PKs = []
                if brad_table_name in all_indexes:
                    for index_info in all_indexes[brad_table_name]:
                        column_name = index_info[-1].split("(")[-1].split(")")[0]
                        table_indexes.append(column_name)
                        if "_brad_source_pkey" in index_info[1]:
                            table_PKs.append(column_name)
                self._catalog[table_name] = {
                    "nrow": nrow[0],
                    "ncol": ncol[0],
                    "indexes": table_indexes,
                    "PKs": table_PKs,
                }
        await sessions.end_session(session_id)

    def check_engine_state(
        self, engine: Engine, sys_metric: Mapping[str, float]
    ) -> bool:
        # We define a set of rules to judge whether the current system is overloaded
        not_overloaded = True
        if engine == Engine.Aurora:
            for param in self._params.aurora_parameters_upper_limit:
                limit = self._params.aurora_parameters_upper_limit[param]
                if limit and param in sys_metric and sys_metric[param] > limit:
                    not_overloaded = False
                    break
            if not_overloaded:
                for param in self._params.aurora_parameters_lower_limit:
                    limit = self._params.aurora_parameters_lower_limit[param]
                    if limit and param in sys_metric and sys_metric[param] < limit:
                        not_overloaded = False
                        break

        if engine == Engine.Redshift:
            for param in self._params.redshift_parameters_upper_limit:
                limit = self._params.redshift_parameters_upper_limit[param]
                if limit and param in sys_metric and sys_metric[param] > limit:
                    not_overloaded = False
                    break
            if not_overloaded:
                for param in self._params.redshift_parameters_lower_limit:
                    limit = self._params.redshift_parameters_lower_limit[param]
                    if limit and param in sys_metric and sys_metric[param] < limit:
                        not_overloaded = False
                        break

        if engine == Engine.Athena:
            # Athena should always be not overloaded
            return True
        return not_overloaded

    def engine_for_sync(self, query: QueryRep, session: Session) -> Engine:
        if self._table_placement_bitmap is None:
            if self._blueprint is not None:
                blueprint = self._blueprint
            else:
                assert self._blueprint_mgr is not None
                blueprint = self._blueprint_mgr.get_blueprint()
            self._table_placement_bitmap = blueprint.table_locations_bitmap()

        valid_locations, only_location = self._filter_on_constraints(
            query, self._table_placement_bitmap, session
        )
        if only_location is not None:
            return only_location

        locations = Engine.from_bitmap(valid_locations)
        assert len(locations) > 1
        ideal_location_rank: List[Engine] = []
        touched_tables = query.tables()
        if (
            len(touched_tables)
            < self._params.ideal_location_lower_limit["redshift_num_table"]
        ):
            ideal_location_rank = [Engine.Aurora, Engine.Redshift, Engine.Athena]
        elif (
            len(touched_tables)
            <= self._params.ideal_location_upper_limit["aurora_num_table"]
        ):
            ideal_location_rank = [Engine.Redshift, Engine.Aurora, Engine.Athena]
            if self._catalog:
                n_rows = []
                n_cols = []
                for table_name in query.tables():
                    if table_name in self._catalog:
                        n_rows.append(self._catalog[table_name]["nrow"])
                        n_cols.append(self._catalog[table_name]["ncol"])
                if (
                    max(n_rows)
                    < self._params.ideal_location_upper_limit["aurora_table_size_max"]
                    and sum(n_rows)
                    < self._params.ideal_location_upper_limit["aurora_table_size_sum"]
                    and max(n_cols)
                    < self._params.ideal_location_upper_limit[
                        "aurora_table_ncolumn_max"
                    ]
                    and sum(n_cols)
                    < self._params.ideal_location_upper_limit[
                        "aurora_table_ncolumn_sum"
                    ]
                ):
                    ideal_location_rank = [
                        Engine.Aurora,
                        Engine.Redshift,
                        Engine.Athena,
                    ]
        elif (
            len(touched_tables)
            <= self._params.ideal_location_upper_limit["redshift_num_table"]
        ):
            ideal_location_rank = [Engine.Redshift, Engine.Athena, Engine.Aurora]
            if self._catalog:
                n_rows = []
                for table_name in query.tables():
                    if table_name in self._catalog:
                        n_rows.append(self._catalog[table_name]["nrow"])
                if (
                    max(n_rows)
                    > self._params.ideal_location_upper_limit["redshift_table_size_max"]
                    and sum(n_rows)
                    > self._params.ideal_location_upper_limit["redshift_table_size_sum"]
                ):
                    ideal_location_rank = [
                        Engine.Athena,
                        Engine.Redshift,
                        Engine.Aurora,
                    ]
        else:
            ideal_location_rank = [Engine.Athena, Engine.Redshift, Engine.Aurora]

        if self._monitor is None:
            for loc in ideal_location_rank:
                if loc in locations:
                    return loc
            # This should be unreachable since len(locations) > 0.
            assert False
        else:
            # Todo(Ziniu): this can be stored in this class to reduce latency
            raw_sys_metric = self._monitor.read_k_most_recent(k=1)
            if raw_sys_metric.empty:
                logger.warning(
                    "Routing without system metrics when we expect to have metrics."
                )
                return ideal_location_rank[0]

            col_name = list(raw_sys_metric.columns)
            col_value = list(raw_sys_metric.values)[0]
            sys_metric = {col_name[i]: col_value[i] for i in range(len(col_value))}
            for loc in ideal_location_rank:
                if loc in locations and self.check_engine_state(loc, sys_metric):
                    return loc

            # In the case of all system are overloaded (time to trigger replan),
            # we assign it to the optimal one. But Athena should not be overloaded at any time
            for loc in ideal_location_rank:
                if loc in locations:
                    return loc

            # Should be unreachable since len(locations) > 0.
            assert False
