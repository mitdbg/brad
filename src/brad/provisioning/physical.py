from .rds import RdsProvisioning
from .redshift import RedshiftProvisioning
from .athena import AthenaProvisioning

from brad.blueprint.blueprint import Blueprint
from brad.config.engine import Engine
from brad.daemon.monitor import Monitor, get_metric_id
import brad.provisioning as provisioning
import json
from importlib.resources import files, as_file
from typing import Dict, Any


# Manages physical provisioning using the logical blueprint.
class PhysicalProvisioning:
    # Initialize physical provisioning.
    def __init__(
        self, monitor: Monitor, initial_blueprint: Blueprint, cluster_ids=None
    ):
        # Take monitor and triggers.
        self._monitor = monitor
        thresholds_file = files(provisioning).joinpath("thresholds.json")
        with as_file(thresholds_file) as file:
            with open(file, "r", encoding="utf8") as data:
                self._trigger_ranges = json.load(data)
        # Setup initial config.
        if cluster_ids is not None:
            aurora_id = cluster_ids[Engine.Aurora]
            redshift_id = cluster_ids[Engine.Redshift]
            athena_id = cluster_ids[Engine.Redshift]
        else:
            aurora_id = f"brad-{initial_blueprint.schema_name()}"
            redshift_id = f"brad-{initial_blueprint.schema_name()}"
            athena_id = f"brad-{initial_blueprint.schema_name()}"
        aurora_instance_type = initial_blueprint.aurora_provisioning().instance_type()
        redshift_instance_type = (
            initial_blueprint.redshift_provisioning().instance_type()
        )
        redshift_instance_count = initial_blueprint.redshift_provisioning().num_nodes()
        self._athena_provisioning = AthenaProvisioning(athena_id=athena_id)
        self._rds_provisioning = RdsProvisioning(
            cluster_name=aurora_id, initial_instance_type=aurora_instance_type
        )
        self._redshift_provisioning = RedshiftProvisioning(
            cluster_name=redshift_id,
            initial_instance_type=redshift_instance_type,
            initial_cluster_size=redshift_instance_count,
        )
        print(f"InitialRedshift: {self._redshift_provisioning}")
        print(f"InitialAurora: {self._rds_provisioning}")
        # Start if not paused.
        self.update_blueprint(new_blueprint=initial_blueprint)
        print(f"InitialRedshift: {self._redshift_provisioning}")
        print(f"InitialAurora: {self._rds_provisioning}")

    # Check if should replan.
    # Currently only reads the previous metric.
    # Can override certain metrics by providing Dict[metric_id -> value]
    def should_trigger_replan(self, overrides=None) -> bool:
        df = self._monitor.read_k_most_recent(1)
        print("TRIGGER REPLAN")
        print(df)
        if df is None:
            return False
        for engine, triggers in self._trigger_ranges.items():
            roles = triggers.get("roles", [""])
            metrics = triggers["metrics"]
            for role in roles:
                for metric_name, lohi in metrics.items():
                    lo, hi = lohi[0], lohi[1]
                    metric_id = get_metric_id(engine, metric_name, "Average", role=role)
                    if metric_id not in df.columns:
                        continue
                    metric_value = df[metric_id][-1]
                    print(f"Metric {metric_id}: {metric_value}")
                    if overrides is not None and metric_id in overrides:
                        metric_value = overrides[metric_id]
                    if metric_value < lo or metric_value > hi:
                        return True

        return False

    # Update physical provisioning.
    def update_blueprint(self, new_blueprint: Blueprint):
        aurora_instance_type = new_blueprint.aurora_provisioning().instance_type()
        aurora_paused = new_blueprint.aurora_provisioning().is_paused()
        redshift_instance_type = new_blueprint.redshift_provisioning().instance_type()
        redshift_instance_count = new_blueprint.redshift_provisioning().num_nodes()
        redshift_paused = new_blueprint.redshift_provisioning().is_paused()
        print("Rescaling Aurora...")
        self._rds_provisioning.rescale(
            immediate=True,
            new_instance_type=aurora_instance_type,
            new_paused=aurora_paused,
        )
        print("Rescaling Redshift...")
        self._redshift_provisioning.rescale(
            new_instance_type=redshift_instance_type,
            new_cluster_size=redshift_instance_count,
            new_paused=redshift_paused,
        )

    # Return aurora connection info (writer address, reader address, port).
    def get_aurora_connection(self):
        self._rds_provisioning.connection_info()

    # Return redshift connection (address, port).
    def get_redshift_connection(self):
        self._redshift_provisioning.connection_info()

    # Return athena's workgroup for issuing queries.
    def get_athena_workgroup(self):
        self._athena_provisioning.get_workgroup()

    # Return all connection infos.
    def connection_info(self) -> Dict[Engine, Any]:
        return {
            Engine.Aurora: self.get_aurora_connection(),
            Engine.Redshift: self.get_redshift_connection(),
            Engine.Athena: self.get_athena_workgroup(),
        }

    # Change specific trigger.
    def change_trigger(self, engine: Engine, metric: str, lo: float, hi: float):
        if engine not in self._trigger_ranges:
            self._trigger_ranges[engine] = {}
        self._trigger_ranges[engine][metric] = [lo, hi]

    # Pause all engines to prevent inactivity costs.
    def pause_all(self):
        self._rds_provisioning.rescale(immediate=True, new_paused=True)
        self._redshift_provisioning.rescale(new_paused=True)
