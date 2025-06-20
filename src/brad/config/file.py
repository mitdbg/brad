import logging
import os
import pathlib
import re
import yaml
from typing import Optional, Dict, Any
from datetime import timedelta

from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.routing.policy import RoutingPolicy

logger = logging.getLogger(__name__)


class ConfigFile:
    @classmethod
    def load_from_new_configs(
        cls, phys_config: str, system_config: str
    ) -> "ConfigFile":
        # Note that this implementation is designed to support backward
        # compatibility (to minimize the invasiveness). We have split our
        # configs into a physical configuration and system configuration.
        #
        # The physical configuration represents deployment-specific configs and
        # credentials and are not meant to be checked in (e.g., for
        # experiments). The system configuration is meant for shared BRAD
        # configurations.

        with open(phys_config, "r", encoding="UTF-8") as file:
            phys_config_dict = yaml.load(file, Loader=yaml.Loader)
        with open(system_config, "r", encoding="UTF-8") as file:
            system_config_dict = yaml.load(file, Loader=yaml.Loader)

        merged = {}
        merged.update(phys_config_dict)
        merged.update(system_config_dict)
        return cls(merged)

    @classmethod
    def load_from_physical_config(cls, phys_config: str) -> "ConfigFile":
        # This implementation is designed to support backward compatibility (to
        # minimize invasiveness). This should be used when only physical
        # config values are needed.
        with open(phys_config, "r", encoding="UTF-8") as file:
            phys_config_dict = yaml.load(file, Loader=yaml.Loader)
        return cls(phys_config_dict)

    @classmethod
    def load(cls, file_path: str) -> "ConfigFile":
        with open(file_path, "r", encoding="UTF-8") as file:
            raw = yaml.load(file, Loader=yaml.Loader)
        return cls(raw)

    def __init__(self, raw_parsed: Dict[str, Any]):
        self._raw = raw_parsed

    @property
    def daemon_log_path(self) -> Optional[pathlib.Path]:
        return self._extract_log_path("daemon_log_file")

    def front_end_log_file(self, worker_index: int) -> Optional[pathlib.Path]:
        log_path = self._extract_log_path("front_end_log_path")
        if log_path is None:
            return None
        return log_path / f"brad_front_end_{worker_index}.log"

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
    def planner_log_path(self) -> Optional[pathlib.Path]:
        return self._extract_log_path("planner_log_path")

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
    def local_logs_path(self) -> pathlib.Path:
        return (
            pathlib.Path(self._raw["local_logs_path"])
            if "local_logs_path" in self._raw
            else pathlib.Path("./query_logs")
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
    def front_end_query_latency_buffer_size(self) -> int:
        return int(self._raw["front_end_query_latency_buffer_size"])

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

    @property
    def disable_table_movement(self) -> bool:
        try:
            return self._raw["disable_table_movement"]
        except KeyError:
            # Table movement disabled by default.
            return True

    @property
    def skip_sync_before_movement(self) -> bool:
        try:
            return self._raw["skip_sync_before_table_movement"]
        except KeyError:
            # Skip by default.
            return True

    @property
    def skip_athena_table_deletion(self) -> bool:
        try:
            return self._raw["skip_athena_table_deletion"]
        except KeyError:
            # Skip by default.
            return True

    @property
    def skip_aurora_table_deletion(self) -> bool:
        try:
            return self._raw["skip_aurora_table_deletion"]
        except KeyError:
            # Skip by default.
            return True

    @property
    def use_preset_redshift_clusters(self) -> bool:
        try:
            # We require that table movement is also disabled. Otherwise we need
            # to keep track of the table state on each preset cluster.
            return (
                self._raw["use_preset_redshift_clusters"]
                and self.disable_table_movement
            )
        except KeyError:
            return False

    def get_preset_redshift_cluster_id(
        self, provisioning: Provisioning
    ) -> Optional[str]:
        """
        If a preset cluster is available for the given provisioning, this method
        returns its cluster ID. Note that this does not check if preset use is
        disabled.
        """
        conn_config = self.get_connection_details(Engine.Redshift)
        if "presets" not in conn_config:
            return None
        for preset in conn_config["presets"]:
            # Check if any of the preset clusters match the given Redshift
            # provisioning.
            if (
                preset["instance_type"] == provisioning.instance_type()
                and preset["num_nodes"] == provisioning.num_nodes()
            ):
                return preset["cluster_id"]
        return None

    def get_connection_details(self, engine: Engine) -> Dict[str, Any]:
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

    def stub_mode_path(self) -> Optional[pathlib.Path]:
        """
        If set, BRAD will run in "stub" mode, where we connect to in memory
        databases instead and use a preset blueprint. This is for debug and
        testing purposes to avoid dependencies on AWS resources.
        """
        if "stub_mode_path" not in self._raw:
            return None
        return pathlib.Path(self._raw["stub_mode_path"])

    def stub_db_path(self) -> pathlib.Path:
        if "stub_db_path" not in self._raw:
            return pathlib.Path("/tmp/brad_db_stub.sqlite")
        else:
            return pathlib.Path(self._raw["stub_db_path"])

    def ui_interface(self) -> str:
        if "ui_interface" in self._raw:
            return self._raw["ui_interface"]
        else:
            return "0.0.0.0"

    def ui_port(self) -> int:
        if "ui_port" in self._raw:
            return self._raw["ui_port"]
        else:
            return 7583

    def result_row_limit(self) -> Optional[int]:
        try:
            return self._raw["result_row_limit"]
        except KeyError:
            return None

    def bootstrap_vdbe_path(self) -> Optional[pathlib.Path]:
        try:
            return pathlib.Path(self._raw["bootstrap_vdbe_path"])
        except KeyError:
            return None

    def disable_query_logging(self) -> bool:
        try:
            return self._raw["disable_query_logging"]
        except KeyError:
            return False

    def vdbe_start_port(self) -> int:
        """
        Returns the port on which the first VDBE will be started. The rest of the
        VDBEs will be started on consecutive ports.
        """
        if "vdbe_start_port" not in self._raw:
            return 9876  # Default
        return int(self._raw["vdbe_start_port"])

    def flight_sql_mode(self) -> Optional[str]:
        try:
            return self._raw["flight_sql_mode"]
        except KeyError:
            # FlightSQL mode is not set.
            return None

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
