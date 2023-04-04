import enum

from brad.config.dbtype import DBType


class Location(str, enum.Enum):
    """
    Represents the location of data in BRAD.
    """

    Aurora = "location_aurora"
    Redshift = "location_redshift"
    S3Iceberg = "location_s3_iceberg"

    def default_engine(self) -> DBType:
        """
        The default engine for processing data in the specified location.
        """
        if self.value == Location.Aurora:
            return DBType.Aurora
        elif self.value == Location.Redshift:
            return DBType.Redshift
        elif self.value == Location.S3Iceberg:
            return DBType.Athena
        else:
            raise AssertionError
