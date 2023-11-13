import operator
import yaml
from typing import List, Tuple, Dict
from functools import reduce
from importlib.resources import files, as_file

import brad.routing as routing
from brad.config.engine import Engine, EngineBitmapValues


class Functionality:
    Geospatial = "geospatial"
    Transaction = "transactions"

    def __init__(self):
        # Read the YAML file
        functionality_yaml = files(routing).joinpath("engine_functionality.yml")
        with as_file(functionality_yaml) as file:
            with open(file, "r", encoding="utf8") as yaml_file:
                data = yaml.load(yaml_file, Loader=yaml.FullLoader)

        # Initialize lists for each database engine's functionalities
        aurora_functionalities = []
        athena_functionalities = []
        redshift_functionalities = []

        # Parse the data into the respective lists
        for engine in data["database_engines"]:
            if engine["name"] == "Aurora":
                aurora_functionalities = engine["functionalities"]
            elif engine["name"] == "Athena":
                athena_functionalities = engine["functionalities"]
            elif engine["name"] == "Redshift":
                redshift_functionalities = engine["functionalities"]

        # Convert to bitmaps
        self.engine_functionalities = [
            (
                EngineBitmapValues[Engine.Athena],
                Functionality.to_bitmap(athena_functionalities),
            ),
            (
                EngineBitmapValues[Engine.Aurora],
                Functionality.to_bitmap(aurora_functionalities),
            ),
            (
                EngineBitmapValues[Engine.Redshift],
                Functionality.to_bitmap(redshift_functionalities),
            ),
        ]

    @staticmethod
    def to_bitmap(functionalities: List[str]) -> int:
        if len(functionalities) == 0:
            return 0
        return reduce(
            # Bitwise OR
            operator.or_,
            map(lambda f: FunctionalityBitmapValues[f], functionalities),
            0,
        )

    def get_engine_functionalities(self) -> List[Tuple[int, int]]:
        """
        Return a bitmap for each engine that states what functionalities the
        engine supports.

        The first value in the tuple is the bitmask representing the the engine.
        The second value in the tuple is the bitmap representing its supported
        functionalities.
        """
        return self.engine_functionalities


FunctionalityBitmapValues: Dict[str, int] = {}
FunctionalityBitmapValues[Functionality.Geospatial] = 0b01
FunctionalityBitmapValues[Functionality.Transaction] = 0b10
