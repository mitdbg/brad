from typing import Dict, Tuple, Optional

from brad.config.engine import Engine, EngineBitmapValues
from brad.query_rep import QueryRep


class Router:
    async def run_setup(self) -> None:
        """
        Should be called before using the router. This is used to set up any
        dynamic state.
        """

    def engine_for(self, query: QueryRep) -> Engine:
        """
        Selects an engine for the provided SQL query.

        NOTE: Implementers currently do not need to consider DML queries. BRAD
        routes all DML queries to Aurora before consulting the router. Thus the
        query passed to this method will always be a read-only query.
        """

        raise NotImplementedError

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
