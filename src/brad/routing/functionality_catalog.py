from typing import List
import operator
import yaml
from functools import reduce
from typing import Dict


class Functionality:
    Geospatial = "geospatial"
    Transaction = "transactions"

    def __init__(self, functionality_yaml="engine_functionality.yml"):
        # Read the YAML file
        with open(functionality_yaml, "r") as yaml_file:
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
        engine_functionality_strings = [
            athena_functionalities,
            aurora_functionalities,
            redshift_functionalities,
        ]
        self.engine_functionalities = [
            Functionality.to_bitmap(f) for f in engine_functionality_strings
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

    def get_engine_functionalities(self) -> List[int]:
        """
        Return a bitmap for each engine that states what functionalities the
        engine supports
        """
        return self.engine_functionalities


FunctionalityBitmapValues: Dict[str, int] = {}
FunctionalityBitmapValues[Functionality.Geospatial] = 0b01
FunctionalityBitmapValues[Functionality.Transaction] = 0b10
