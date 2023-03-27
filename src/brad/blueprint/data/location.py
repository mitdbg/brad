import enum


class Location(str, enum.Enum):
    """
    Represents the location of data in BRAD.
    """

    Aurora = "location_aurora"
    Redshift = "location_redshift"
    S3Iceberg = "location_s3_iceberg"
