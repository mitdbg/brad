from typing import Dict, Tuple, Optional

from functionality_catalog import Functionality
from brad.data_stats.estimator import Estimator
from brad.config.engine import Engine, EngineBitmapValues
from brad.query_rep import QueryRep


class Router:

    def __init__(self):
        self.functionality_catalog = Functionality()

    async def run_setup(self, estimator: Optional[Estimator] = None) -> None:
        """
        Should be called before using the router. This is used to set up any
        dynamic state.

        If the routing policy needs an estimator, one should be provided here.
        """

    async def engine_for(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.

        NOTE: Implementers currently do not need to consider DML queries. BRAD
        routes all DML queries to Aurora before consulting the router. Thus the
        query passed to this method will always be a read-only query.

        You should override this method if the routing policy needs to depend on
        any asynchronous methods.
        """
        return self.engine_for_sync(query)

    def engine_for_sync(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.

        NOTE: Implementers currently do not need to consider DML queries. BRAD
        routes all DML queries to Aurora before consulting the router. Thus the
        query passed to this method will always be a read-only query.
        """
        raise NotImplementedError

    def _run_functionality_routing(
        self, query: QueryRep
    ) -> Tuple[int, Optional[Engine]]:
        """
        Based on the functinalities required by the query (e.g. geospatial),
        compute the set of engines that are able to serve this query.
        """

        # Bitmap describing what functionality is required for running query
        req_bitmap = query.get_required_functionality()

        # Bitmap for each engine which features it supports
        engine_support = self.functionality_catalog.get_engine_functionalities()
        engines = [Engine.Athena, Engine.Aurora, Engine.Redshift]

        # Narrow down the valid engines that can run the query, based on the
        # engine functionality
        valid_locations_list = []
        for engine, sup_bitmap in zip(engines, engine_support):

            query_supported = (~req_bitmap | (req_bitmap & sup_bitmap)) == -1

            if query_supported:
                valid_locations_list.append(engine)

        valid_locations = Engine.to_bitmap(valid_locations_list)

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
