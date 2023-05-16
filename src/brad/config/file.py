import yaml
from typing import Optional, Any, Dict
from datetime import timedelta

from brad.config.engine import Engine
from brad.routing.policy import RoutingPolicy


class ConfigFile:
    def __init__(self, path: str):
        self._raw_path = path
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    def get_cluster_ids(self) -> Dict[Engine, str]:
        return {
            Engine.Aurora: self.aurora_cluster_id,
            Engine.Redshift: self.redshift_cluster_id,
            Engine.Athena: "brad-db0", # TODO(Amadou): I don't want to break existing configs. Coordinate with Geoff on this. 
        }

    @property
    def raw_path(self) -> str:
        return self._raw_path

    @property
    def daemon_log_path(self) -> Optional[str]:
        return self._raw["daemon_log_file"] if "daemon_log_file" in self._raw else None

    @property
    def planner_log_path(self) -> str:
        return self._raw["planner_log_path"] if "planner_log_path" in self._raw else "."

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
        return _ensure_slash_terminated(self._raw[Engine.Athena]["s3_data_path"])

    @property
    def redshift_s3_iam_role(self) -> str:
        """Needed when importing data from S3."""
        return self._raw[Engine.Redshift]["s3_iam_role"]

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

    @property
    def redshift_cluster_id(self) -> str:
        return self._raw[Engine.Redshift]["host"].split(".")[0]

    @property
    def aurora_cluster_id(self) -> str:
        return self._raw[Engine.Aurora]["host"].split(".")[0]

    @property
    def forecasting_epoch(self) -> timedelta:
        epoch = self._raw["forecasting_epoch_length"]
        return timedelta(
            weeks=epoch["weeks"],
            days=epoch["days"],
            hours=epoch["hours"],
            minutes=epoch["minutes"],
        )

    def get_odbc_connection_string(self, db: Engine, schema_name: Optional[str], conn_info: Any) -> str:
        if db not in self._raw:
            raise AssertionError("Unhandled database type: " + str(db))

        config = self._raw[db]
        if db is Engine.Athena:
            if conn_info is None:
                workgroup = None
                s3_path = config["s3_output_path"]
            else:
                (workgroup, s3_path) = conn_info
            cstr = "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};".format(
                config["odbc_driver"],
                config["aws_region"],
                s3_path,
                config["access_key"],
                config["access_key_secret"],
            )
            if workgroup is not None:
                cstr += f"Workgroup={workgroup};"
            if schema_name is not None:
                cstr += "Schema={};".format(schema_name)
        elif db is Engine.Aurora:
            (read_only, conn_info) = conn_info
            if conn_info is None:
                host = config["host"]
                port = config["port"]
            else:
                (writer_host, reader_host, port) = conn_info
                if read_only:
                    host = reader_host
                else:
                    host = writer_host
            cstr = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};".format(
                config["odbc_driver"],
                host,
                port,
                config["user"],
                config["password"],
            )
            if schema_name is not None:
                cstr += "Database={};".format(schema_name)
        elif db is Engine.Redshift:
            if conn_info is None:
                host = config["host"]
                port = config["port"]
            else:
                (host, port) = conn_info
            cstr = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database=dev;".format(
                config["odbc_driver"],
                host,
                port,
                config["user"],
                config["password"],
            )
        return cstr


def _ensure_slash_terminated(candidate: str) -> str:
    if not candidate.endswith("/"):
        return candidate + "/"
    else:
        return candidate
