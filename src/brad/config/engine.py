import enum


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
