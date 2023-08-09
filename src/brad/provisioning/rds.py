import asyncio
import boto3
import time
import logging
from typing import Any, Dict

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.blueprint.provisioning import Provisioning

logger = logging.getLogger(__name__)


class RdsProvisioningManager:
    def __init__(self, config: ConfigFile) -> None:
        self._config = config
        self._rds = boto3.client(
            "rds",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_access_key_secret,
        )

    async def run_primary_failover(
        self, cluster_id: str, new_primary_identifier: str
    ) -> None:
        def do_failover():
            self._rds.failover_db_cluster(
                DBClusterIdentifier=cluster_id,
                TargetDBInstanceIdentifier=new_primary_identifier,
            )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, do_failover)

    async def create_replica(
        self,
        cluster_id: str,
        instance_id: str,
        provisioning: Provisioning,
        wait_until_available: bool = True,
    ) -> None:
        def do_create_replica():
            monitoring_role_arn = self._config.get_connection_details(Engine.Aurora)[
                "monitoring_role_arn"
            ]
            self._rds.create_db_instance(
                DBClusterIdentifier=cluster_id,
                DBInstanceIdentifier=instance_id,
                PubliclyAccessible=True,
                DBInstanceClass=provisioning.instance_type(),
                Engine="aurora-postgresql",
                EnablePerformanceInsights=True,
                MonitoringInterval=60,
                MonitoringRoleArn=monitoring_role_arn,
            )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, do_create_replica)

        if wait_until_available:
            # Need a slight delay to ensure the instance's state change is
            # updated.
            await asyncio.sleep(5)
            await self.wait_until_instance_is_available(instance_id)

    async def delete_replica(self, instance_id: str) -> None:
        def do_delete():
            self._rds.delete_db_instance(
                DBInstanceIdentifier=instance_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, do_delete)

    async def wait_until_instance_is_available(
        self, instance_id: str, polling_interval: float = 20
    ) -> None:
        while True:
            response = await self._describe_db_instance(instance_id)
            instance = response["DBInstances"][0]
            status = instance["DBInstanceStatus"]
            if status == "available":
                break
            logger.debug(
                "Waiting for Aurora instance %s to become available...", instance_id
            )
            await asyncio.sleep(polling_interval)

    async def wait_until_cluster_is_available(
        self, cluster_id: str, polling_interval: float = 20
    ) -> None:
        while True:
            response = await self._describe_db_cluster(cluster_id)
            cluster = response["DBClusters"][0]
            status = cluster["Status"]
            # Check if status is stable.
            if status == "available":
                break
            logger.debug(
                "Waiting for Aurora cluster %s to become available...", cluster_id
            )
            await asyncio.sleep(polling_interval)

    async def start_cluster(
        self, cluster_id: str, wait_until_available: bool = True
    ) -> None:
        def do_start():
            self._rds.start_db_cluster(
                DBClusterIdentifier=cluster_id,
            )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, do_start)

        if wait_until_available:
            await self.wait_until_cluster_is_available(cluster_id)

    async def pause_cluster(self, cluster_id: str) -> None:
        def do_pause():
            self._rds.pause_db_cluster(
                DBClusterIdentifier=cluster_id,
            )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, do_pause)

    async def change_instance_type(
        self,
        instance_id: str,
        provisioning: Provisioning,
        wait_until_available: bool = True,
    ) -> None:
        def do_change():
            self._rds.modify_db_instance(
                DBInstanceIdentifier=instance_id,
                DBInstanceClass=provisioning.instance_type(),
                ApplyImmediately=True,
            )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, do_change)

        if wait_until_available:
            # Need a slight delay to ensure the instance's state change is
            # updated.
            await asyncio.sleep(5)
            await self.wait_until_instance_is_available(instance_id)

    async def _describe_db_instance(self, instance_id: str) -> Dict[str, Any]:
        def do_describe():
            return self._rds.describe_db_instances(
                DBInstanceIdentifier=instance_id,
            )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, do_describe)

    async def _describe_db_cluster(self, cluster_id: str) -> Dict[str, Any]:
        def do_describe():
            return self._rds.describe_db_clusters(
                DBClusterIdentifier=cluster_id,
            )

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, do_describe)


class RdsProvisioning:
    # Initialize provisioning.
    # If cluster does not exist, you must specify its initial state for it to be created.
    def __init__(
        self,
        cluster_name="brad-cluster0",
        initial_instance_type=None,
        initial_num_nodes=None,
    ):
        self.cluster_name = cluster_name
        self.instance_type = initial_instance_type
        self.num_nodes = initial_num_nodes
        self.paused = None
        self.address = None
        self.reader_address = None
        self.port = None
        self.rds = boto3.client("rds")
        self.redeploy(immediate=False)

    # String representation.
    def __str__(self) -> str:
        return f"RdsCluster(name={self.cluster_name}, instance_type={self.instance_type}, num_nodes={self.num_nodes}, paused={self.paused}, address={self.address}, port={self.port})"

    # Return connection info (writer address, reader address, port).
    def connection_info(self):
        return (self.address, self.reader_address, self.port)

    # Delete cluster.
    def delete_cluster(self):
        self.rds.delete_cluster(
            DBClusterIdentifier=self.cluster_name,
            SkipFinalClusterSnapshot=True,
        )

    # Rescale a cluster.
    # This will wait until new cluster's state is avalaible.
    # Start it in a new thread if thou dost not want to wait.
    def rescale(
        self,
        immediate=True,
        new_instance_type=None,
        new_num_nodes=None,
        new_paused=None,
    ):
        if new_instance_type is not None:
            self.instance_type = new_instance_type
        if new_paused is not None:
            self.paused = new_paused
        if new_num_nodes is not None:
            self.num_nodes = new_num_nodes
        self.redeploy(immediate)

    # Reconcile cluster state.
    def reconcile_cluster_state(self):
        while True:
            try:
                print("reconcile_cluster_state.")
                response = self.rds.describe_db_clusters(
                    DBClusterIdentifier=self.cluster_name,
                )
                logging.info(f"reconcile_cluster_state. Response: {response}")
                cluster = response["DBClusters"][0]
                status = cluster["Status"]
                # Set default values.
                self.address = cluster["Endpoint"]
                self.reader_address = cluster["ReaderEndpoint"]
                self.port = cluster["Port"]
                if self.paused is None:
                    self.paused = status in ("stopping", "stopped")
                if self.num_nodes is None:
                    self.num_nodes = len(cluster["DBClusterMembers"])
                # Check if status is stable.
                if status != "available" and status != "stopped":
                    logging.info(
                        f"Rds Cluster {self.cluster_name}. Status: {status}. Waiting..."
                    )
                    time.sleep(5.0)
                    continue
                # Reconcile running state.
                if self.paused and status == "stopped":
                    logging.info(f"Rds Cluster {self.cluster_name}. Is in right state.")
                    return
                if not (self.paused) and status == "available":
                    logging.info(f"Rds Cluster {self.cluster_name}. Is in right state.")
                    return
                if self.paused and status == "available":
                    # Should pause.
                    logging.info(f"Rds Cluster {self.cluster_name}. Pausing...")
                    resp = self.rds.stop_db_cluster(
                        DBClusterIdentifier=self.cluster_name,
                    )
                    logging.info(f"Pause Resp: {resp}")
                    time.sleep(5.0)
                    continue
                if not (self.paused) and status == "stopped":
                    # Should start.
                    logging.info(f"Rds Instance {self.cluster_name}. Restarting...")
                    self.rds.start_db_cluster(
                        DBClusterIdentifier=self.cluster_name,
                    )
                    time.sleep(5.0)
                    continue
            except Exception as _e:
                pass
            try:
                logging.info(f"Rds Cluster {self.cluster_name}. Creating...")
                self.rds.create_db_cluster(
                    DBClusterIdentifier=self.cluster_name,
                    Engine="aurora-postgresql",
                    EngineMode="provisioned",
                    DatabaseName="dev",
                    MasterUsername="brad",
                    MasterUserPassword="BradBrad123",
                )
            except Exception as e:
                e_str = f"{e}"
                if "AlreadyExists" in e_str:
                    continue
                else:
                    raise e

    # Get writer or create if not exists.
    def get_or_create_instance(self, instance_id):
        while True:
            try:
                response = self.rds.describe_db_clusters(
                    DBClusterIdentifier=self.cluster_name,
                )
                logging.info(f"get_or_create_writer. Response: {response}")
                cluster = response["DBClusters"][0]
                status = cluster["Status"]
                # Check if status is stable.
                if status != "available" and status != "stopped":
                    logging.info(
                        f"Rds Cluster {self.cluster_name}. Status: {status}. Waiting..."
                    )
                    time.sleep(5.0)
                    continue
                # Find instance.
                members = [
                    x["DBInstanceIdentifier"] for x in cluster["DBClusterMembers"]
                ]
                if instance_id in members:
                    return
                # Create Writer Instance.
                logging.info(f"RDS. Creating Instance {instance_id}...")
                self.rds.create_db_instance(
                    DBClusterIdentifier=self.cluster_name,
                    # DBName="dev",
                    DBInstanceIdentifier=instance_id,
                    PubliclyAccessible=True,
                    DBInstanceClass=self.instance_type,
                    Engine="aurora-postgresql",
                )
                time.sleep(5.0)
                continue
            except Exception as e:
                e_str = f"{e}"
                if "AlreadyExists" in e_str:
                    print("Rds Instance already exists...")
                    time.sleep(5.0)
                    return
                else:
                    raise e

    # Kill Instance
    def kill_instance_if_exists(self, instance_id):
        try:
            self.rds.delete_db_instance(
                DBInstanceIdentifier=instance_id,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )
        except Exception as e:
            e_str = f"{e}"
            if "NotFound" in e_str:
                print(f"Rds Instance {instance_id} already does not exits.")
                return
            else:
                raise e

    # Reconcile instance state.
    def reconcile_instance(self, instance_id, immediate):
        while True:
            try:
                response = self.rds.describe_db_instances(
                    DBInstanceIdentifier=instance_id,
                )
                logging.debug(f"RDS reconcile_writer. Response: {response}")
                instance = response["DBInstances"][0]
                status = instance["DBInstanceStatus"]
                curr_instance_type = instance["DBInstanceClass"]
                # Set default variables.
                if self.instance_type is None:
                    self.instance_type = curr_instance_type
                # Check if status is stable.
                if status != "available" and status != "stopped":
                    logging.info(
                        f"Rds Cluster {self.cluster_name}. Instance {instance_id} Status: {status}. Waiting..."
                    )
                    time.sleep(5.0)
                    continue
                # Reconcile expected instance type and count with current ones.
                if curr_instance_type == self.instance_type:
                    # Cluster is as it should be. Return.
                    logging.info(f"Rds Cluster {self.cluster_name}. Is in right state.")
                    return
                # Must resize cluster.
                logging.info(
                    f"Rds Cluster {self.cluster_name}. Resizing {instance_id} from {curr_instance_type} to {self.instance_type}..."
                )
                self.rds.modify_db_instance(
                    DBInstanceIdentifier=instance_id,
                    DBInstanceClass=self.instance_type,
                    ApplyImmediately=immediate,
                )
                # Next iteration of the loop will wait for availability.
                time.sleep(5.0)
                continue

            except Exception as e:
                # Should not happend.
                print(f"{e}")
                raise e

    # Redeploy. Used for initialization and rescaling.
    def redeploy(self, immediate):
        # Create cluster if not exist.
        # These functions are intentionally separated to make them easy to write.
        # They contain redundant API calls; no big deal.
        self.reconcile_cluster_state()
        if self.paused:
            return
        active_instances = [i for i in range(0, self.num_nodes)]
        dead_instances = [i for i in range(self.num_nodes, 16)]
        # TODO(Amadou): Stupidly Parallelizable.
        for i in active_instances:
            instance_id = f"{self.cluster_name}-brad{i}"
            self.get_or_create_instance(instance_id=instance_id)
            self.reconcile_instance(instance_id=instance_id, immediate=immediate)
        for i in dead_instances:
            instance_id = f"{self.cluster_name}-brad{i}"
            self.kill_instance_if_exists(instance_id=instance_id)


if __name__ == "__main__":
    # Get or create cluster.
    try:
        rd = RdsProvisioning(cluster_name="brad-cluster0")
    except Exception as _e:
        rd = RdsProvisioning(
            cluster_name="brad-cluster0", initial_instance_type="db.r6g.large"
        )
    # Change Num Nodes.
    num_nodes = rd.num_nodes
    if num_nodes == 1:
        num_nodes = 2
    else:
        num_nodes = 1
    # Change instance type.
    instance_type = rd.instance_type
    if instance_type == "db.r6g.large":
        instance_type = "db.r6g.xlarge"
    else:
        instance_type = "db.r6g.large"
    rd.rescale(
        immediate=True,
        new_paused=False,
        new_instance_type=instance_type,
        new_num_nodes=num_nodes,
    )
    print(rd)
    # Pause.
    rd.rescale(immediate=True, new_paused=True)
    print(rd)
