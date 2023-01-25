import enum


class DBType(str, enum.Enum):
    Athena = "athena"
    Aurora = "aurora"
    Redshift = "redshift"
