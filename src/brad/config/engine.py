import enum
import operator
from typing import List
from functools import reduce


class Engine(str, enum.Enum):
    Athena = "athena"
    Aurora = "aurora"
    Redshift = "redshift"

    @staticmethod
    def from_str(candidate: str) -> "Engine":
        if candidate == Engine.Athena.value:
            return Engine.Athena
        elif candidate == Engine.Aurora.value:
            return Engine.Aurora
        elif candidate == Engine.Redshift.value:
            return Engine.Redshift
        else:
            raise ValueError("Unrecognized engine {}".format(candidate))

    @staticmethod
    def to_bitmap(engines: List["Engine"]) -> int:
        if len(engines) == 0:
            return 0
        return reduce(
            # Bitwise OR
            operator.or_,
            map(lambda eng: EngineBitmapValues[eng], engines),
            0,
        )

    @staticmethod
    def from_bitmap(engines: int) -> List["Engine"]:
        results = []
        for engine, v in EngineBitmapValues.items():
            if v & engines != 0:
                results.append(engine)
        return results

    @staticmethod
    def bitmap_all() -> int:
        return 0b111


EngineBitmapValues = {}
EngineBitmapValues[Engine.Athena] = 0b001
EngineBitmapValues[Engine.Aurora] = 0b010
EngineBitmapValues[Engine.Redshift] = 0b100
