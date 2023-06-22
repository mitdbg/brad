from typing import Dict

from .model_wrap import ModelWrap
from brad.config.engine import Engine, EngineBitmapValues
from brad.routing import Router
from brad.query_rep import QueryRep


class ForestRouter(Router):
    def __init__(
        self,
        model: ModelWrap,
        table_placement_bitmap: Dict[str, int],
    ) -> None:
        self._model = model
        self._table_placement_bitmap = table_placement_bitmap

    def engine_for(self, query: QueryRep) -> Engine:
        # Narrow down the valid engines that can run the query, based on the
        # table placement.
        valid_locations = Engine.bitmap_all()
        for table_name_str in query.tables():
            try:
                valid_locations &= self._table_placement_bitmap[table_name_str]
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
                return Engine.Aurora
            elif (EngineBitmapValues[Engine.Redshift] & valid_locations) != 0:
                return Engine.Redshift
            elif (EngineBitmapValues[Engine.Athena] & valid_locations) != 0:
                return Engine.Athena
            else:
                raise RuntimeError("Unsupported bitmap value " + str(valid_locations))

        # Multiple locations possible. Use the model to figure out which location to use.
        preferred_locations = self._model.engine_for(query)

        for loc in preferred_locations:
            if (EngineBitmapValues[loc] & valid_locations) != 0:
                return loc

        # This should be unreachable. The model must rank all engines, and we
        # know >= 2 engines can support this query.
        raise AssertionError
