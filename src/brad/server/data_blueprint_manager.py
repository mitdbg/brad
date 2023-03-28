from typing import Optional

from brad.blueprint.data import DataBlueprint
from brad.config.file import ConfigFile


class DataBlueprintManager:
    """
    Utility class used for loading and providing access to the current data blueprint.
    """

    def __init__(self, config: ConfigFile, schema_name: str):
        self._config = config
        self._schema_name = schema_name
        self._blueprint: Optional[DataBlueprint] = None

    async def load(self) -> None:
        pass

    @property
    def schema_name(self) -> str:
        return self._schema_name

    def get(self) -> DataBlueprint:
        assert self._blueprint is not None
        return self._blueprint
