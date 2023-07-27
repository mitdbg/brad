import logging
import os
import pathlib
import re
import yaml
from typing import Optional, Dict
from datetime import timedelta

from brad.config.engine import Engine
from brad.routing.policy import RoutingPolicy

logger = logging.getLogger(__name__)


class ConfigFile:
    def __init__(self, path: str):
        self._raw_path = path
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    def get_cluster_ids(self) -> Dict[Engine, str]:
        return {
            Engine.Aurora: self.aurora_cluster_id,
            Engine.Redshift: self.redshift_cluster_id,
            Engine.Athena: "brad-db0",  # TODO(Amadou): I don't want to break existing configs. Coordinate with Geoff on this.
        }

    @property
    def raw_path(self) -> str:
        return self._raw_path

    @property
    def daemon_log_path(self) -> Optional[str]:
        return self._raw["daemon_log_file"] if "daemon_log_file" in self._raw else None

    def front_end_log_file(self, worker_index: int) -> Optional[pathlib.Path]:
        if "front_end_log_path" in self._raw:
            prefix = pathlib.Path(self._raw["front_end_log_path"])
            return prefix / f"brad_front_end_{worker_index}.log"
        else:
            return None

    def metrics_log_path(self) -> Optional[pathlib.Path]:
        return self._extract_log_path("metrics_log_path")

    @property
    def front_end_interface(self) -> str:
        return self._raw["front_end_interface"]

    @property
    def front_end_port(self) -> int:
        return int(self._raw["front_end_port"])

    @property
    def num_front_ends(self) -> int:
        return int(self._raw["num_front_ends"])

    @property
    def planner_log_path(self) -> str:
        return self._raw["planner_log_path"] if "planner_log_path" in self._raw else "."

    @property
    def athena_s3_data_path(self) -> str:
        return _ensure_slash_terminated(self._raw[Engine.Athena]["s3_data_path"])

    @property
    def athena_s3_output_path(self) -> str:
        return _ensure_slash_terminated(self._raw[Engine.Athena]["s3_output_path"])

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
    def s3_assets_bucket(self) -> str:
        return self._raw["s3_assets_bucket"]

    @property
    def s3_assets_path(self) -> str:
        return _ensure_slash_terminated(self._raw["s3_assets_path"])

    @property
    def s3_extract_bucket(self) -> str:
        """Needed when exporting data from Aurora to S3."""
        return self._raw["s3_extract_bucket"]

    @property
    def s3_extract_path(self) -> str:
        """Needed when exporting data from Aurora to S3."""
        return _ensure_slash_terminated(self._raw["s3_extract_path"])

    @property
    def s3_logs_bucket(self) -> str:
        return self._raw["s3_logs_bucket"]

    @property
    def s3_logs_path(self) -> str:
        return _ensure_slash_terminated(self._raw["s3_logs_path"])

    @property
    def local_logs_path(self) -> str:
        return (
            self._raw["local_logs_path"]
            if "local_logs_path" in self._raw
            else "./query_logs"
        )

    @property
    def s3_extract_region(self) -> str:
        """Needed when exporting data from Aurora to S3."""
        return self._raw["s3_extract_region"]

    @property
    def data_sync_period_seconds(self) -> float:
        return float(self._raw["data_sync_period_seconds"])

    @property
    def front_end_metrics_reporting_period_seconds(self) -> float:
        return float(self._raw["front_end_metrics_reporting_period_seconds"])

    @property
    def routing_policy(self) -> RoutingPolicy:
        return RoutingPolicy.from_str(self._raw["routing_policy"])

    @property
    def redshift_cluster_id(self) -> str:
        return self._raw[Engine.Redshift.value]["cluster_id"]

    @property
    def aurora_cluster_id(self) -> str:
        return self._raw[Engine.Aurora.value]["cluster_id"]

    @property
    def epoch_length(self) -> timedelta:
        epoch = self._raw["epoch_length"]
        return timedelta(
            weeks=epoch["weeks"],
            days=epoch["days"],
            hours=epoch["hours"],
            minutes=epoch["minutes"],
        )

    @property
    def txn_log_prob(self) -> float:
        return float(self._raw["txn_log_prob"])

    def get_connection_details(self, engine: Engine) -> Dict[str, str]:
        """
        Returns the raw configuration details provided for an engine.
        """
        if engine not in self._raw:
            raise AssertionError("Unhandled engine: " + str(engine))
        return self._raw[engine]

    def get_sidecar_db_details(self) -> Dict[str, str]:
        if "sidecar_db" not in self._raw:
            raise RuntimeError("Missing connection details for the Sidecar DBMS.")
        return self._raw["sidecar_db"]

    def _extract_log_path(self, config_key: str) -> Optional[pathlib.Path]:
        if config_key not in self._raw:
            return None
        raw_value = self._raw[config_key]

        if _ENV_VAR_REGEX.match(raw_value) is not None:
            # Treat it as an environment variable.
            if raw_value not in os.environ:
                logger.warning(
                    "Specified an environment variable '%s' for config '%s', but the variable was not set.",
                    raw_value,
                    config_key,
                )
                return None
            return pathlib.Path(os.environ[raw_value])
        else:
            # Treat is as a path.
            return pathlib.Path(raw_value)


def _ensure_slash_terminated(candidate: str) -> str:
    if not candidate.endswith("/"):
        return candidate + "/"
    else:
        return candidate


_ENV_VAR_REGEX = re.compile("[A-Z][A-Z0-9_]*")
