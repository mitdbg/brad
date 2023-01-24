import enum


class DBType(str, enum.Enum):
    Aurora = "aurora"
    Redshift = "redshift"
