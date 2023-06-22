from typing import Optional, Dict

from .model_wrap import ModelWrap
from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.config.engine import Engine, EngineBitmapValues
from brad.routing import Router
from brad.server.blueprint_manager import BlueprintManager
from brad.query_rep import QueryRep


class ForestRouter(Router):
    def __init__(
        self,
        schema_name: str,
        assets: AssetManager,
        # One of `blueprint_mgr`, `blueprint`, and `table_placement_bitmap` must not be `None`.
        blueprint_mgr: Optional[BlueprintManager] = None,
        blueprint: Optional[Blueprint] = None,
        table_placement_bitmap: Optional[Dict[str, int]] = None,
    ) -> None:
        self._schema_name = schema_name
        self._model: Optional[ModelWrap] = None
        self._assets = assets

        self._blueprint_mgr = blueprint_mgr
        self._blueprint = blueprint
        self._table_placement_bitmap = table_placement_bitmap

    async def run_setup(self) -> None:
        # Load the model.
        serialized_model = await self._assets.load(
            _SERIALIZED_KEY.format(schema_name=self._schema_name)
        )
        self._model = ModelWrap.from_pickle_bytes(serialized_model)

        # Load the table placement if it was not provided.
        if self._table_placement_bitmap is None:
            if self._blueprint is None:
                assert self._blueprint_mgr is not None
                self._blueprint = self._blueprint_mgr.get_blueprint()

            self._table_placement_bitmap = self._blueprint.table_locations_bitmap()

    def engine_for(self, query: QueryRep) -> Engine:
        assert self._model is not None
        assert self._table_placement_bitmap is not None

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

    def persist_sync(self) -> None:
        assert self._model is not None
        self.static_persist_sync(self._model, self._schema_name, self._assets)

    @staticmethod
    def static_persist_sync(
        model: ModelWrap, schema_name: str, assets: AssetManager
    ) -> None:
        key = _SERIALIZED_KEY.format(schema_name=schema_name)
        serialized = model.to_pickle()
        assets.persist_sync(key, serialized)


_SERIALIZED_KEY = "{schema_name}/forest_router.pickle"
