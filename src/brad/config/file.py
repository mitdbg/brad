import yaml
from typing import Optional

from brad.config.dbtype import DBType
from brad.routing.policy import RoutingPolicy


class ConfigFile:
    def __init__(self, path: str):
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    @property
    def server_interface(self) -> str:
        return self._raw["server_interface"]

    @property
    def server_port(self) -> int:
        return int(self._raw["server_port"])

    @property
    def server_daemon_port(self) -> int:
        return int(self._raw["server_daemon_port"])

    @property
    def athena_s3_data_path(self) -> str:
        return _ensure_slash_terminated(self._raw[DBType.Athena]["s3_data_path"])

    @property
    def redshift_s3_iam_role(self) -> str:
        """Needed when importing data from S3."""
        return self._raw[DBType.Redshift]["s3_iam_role"]

    @property
    def aws_access_key(self) -> str:
        return self._raw["aws_access_key"]

    @property
    def aws_access_key_secret(self) -> str:
        return self._raw["aws_access_key_secret"]

    @property
    def s3_metadata_bucket(self) -> str:
        return self._raw["s3_metadata_bucket"]

    @property
    def s3_metadata_path(self) -> str:
        return _ensure_slash_terminated(self._raw["s3_metadata_path"])

    @property
    def s3_extract_bucket(self) -> str:
        """Needed when exporting data from Aurora to S3."""
        return self._raw["s3_extract_bucket"]

    @property
    def s3_extract_path(self) -> str:
        """Needed when exporting data from Aurora to S3."""
        return _ensure_slash_terminated(self._raw["s3_extract_path"])

    @property
    def s3_extract_region(self) -> str:
        """Needed when exporting data from Aurora to S3."""
        return self._raw["s3_extract_region"]

    @property
    def data_sync_period_seconds(self) -> float:
        return float(self._raw["data_sync_period_seconds"])

    @property
    def routing_policy(self) -> RoutingPolicy:
        return RoutingPolicy.from_str(self._raw["routing_policy"])

    def get_odbc_connection_string(self, db: DBType, schema_name: Optional[str]) -> str:
        if db not in self._raw:
            raise AssertionError("Unhandled database type: " + str(db))

        config = self._raw[db]
        if db is DBType.Athena:
            cstr = "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};".format(
                config["odbc_driver"],
                config["aws_region"],
                config["s3_output_path"],
                config["access_key"],
                config["access_key_secret"],
            )
            if schema_name is not None:
                cstr += "Schema={};".format(schema_name)
            return cstr

        elif db is DBType.Aurora or db is DBType.Redshift:
            cstr = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};".format(
                config["odbc_driver"],
                config["host"],
                config["port"],
                config["user"],
                config["password"],
            )
            if schema_name is not None:
                cstr += "Database={};".format(schema_name)
            elif db is DBType.Redshift:
                # Redshift requires a database name to be specified. As far as
                # we know, there is always a `dev` database. We connect without
                # a database when we are bootstrapping a new database.
                cstr += "Database=dev;"
            return cstr


def _ensure_slash_terminated(candidate: str) -> str:
    if not candidate.endswith("/"):
        return candidate + "/"
    else:
        return candidate
