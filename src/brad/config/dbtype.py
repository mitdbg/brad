import enum


class DBType(str, enum.Enum):
    Athena = "athena"
    Aurora = "aurora"
    Redshift = "redshift"

    @staticmethod
    def from_str(candidate: str) -> "DBType":
        if candidate == DBType.Athena.value:
            return DBType.Athena
        elif candidate == DBType.Aurora.value:
            return DBType.Aurora
        elif candidate == DBType.Redshift.value:
            return DBType.Redshift
        else:
            raise ValueError("Unrecognized DB type {}".format(candidate))
