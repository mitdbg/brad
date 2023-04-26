import boto3
import time
import logging
import os

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class RdsProvisioning:
    # Initialize provisioning.
    # If cluster does not exist, you must specify its initial state for it to be created.
    def __init__(self, cluster_name="brad-cluster0", initial_instance_type=None):
        self.cluster_name = cluster_name
        self.instance_type = initial_instance_type
        self.paused = None
        self.address = None
        self.reader_address = None
        self.port = None
        self.rds = boto3.client("rds")
        self.redeploy(immediate=False)

    # String representation.
    def __str__(self) -> str:
        return f"RdsCluster(name={self.cluster_name}, instance_type={self.instance_type}, paused={self.paused}, address={self.address}, port={self.port})"

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
    def rescale(self, immediate=True, new_instance_type=None, new_paused=None):
        if new_instance_type is not None:
            self.instance_type = new_instance_type
        if new_paused is not None:
            self.paused = new_paused
        self.redeploy(immediate)

    # Reconcile cluster state.
    def reconcile_cluster_state(self):
        while True:
            try:
                response = self.rds.describe_db_clusters(
                    DBClusterIdentifier=self.cluster_name,
                )
                logging.debug(f"reconcile_cluster_state. Response: {response}")
                cluster = response["DBClusters"][0]
                status = cluster["Status"]
                # Set default values.
                self.address = cluster["Endpoint"]
                self.reader_address = cluster["ReaderEndpoint"]
                self.port = cluster["Port"]
                if self.paused is None:
                    self.paused = status in ("stopping", "stopped")
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
                    self.rds.stop_db_cluster(
                        DBClusterIdentifier=self.cluster_name,
                    )
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
    def get_or_create_writer(self):
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
                # Find writer.
                members = cluster["DBClusterMembers"]
                members = [
                    (x["DBInstanceIdentifier"], x["IsClusterWriter"]) for x in members
                ]
                for member_id, is_writer in members:
                    if is_writer:
                        return member_id
                # Create Writer Instance.
                logging.info("RDS. Creating Writer Instance...")
                self.rds.create_db_instance(
                    DBClusterIdentifier=self.cluster_name,
                    # DBName="dev",
                    DBInstanceIdentifier=f"{self.cluster_name}-brad-writer",
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
                    continue
                else:
                    raise e

    # Reconcile writer state.
    def reconcile_writer(self, writer_id, immediate):
        while True:
            try:
                response = self.rds.describe_db_instances(
                    DBInstanceIdentifier=writer_id,
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
                        f"Rds Cluster {self.cluster_name}. Status: {status}. Waiting..."
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
                    f"Rds Cluster {self.cluster_name}. Resizing to ({self.instance_type})..."
                )
                self.rds.modify_db_instance(
                    DBInstanceIdentifier=writer_id,
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
        write_id = self.get_or_create_writer()
        self.reconcile_writer(write_id, immediate)


if __name__ == "__main__":
    # Get or create cluster.
    try:
        rd = RdsProvisioning(cluster_name="brad-cluster0")
    except Exception as _e:
        rd = RdsProvisioning(
            cluster_name="brad-cluster0", initial_instance_type="db.r6g.large"
        )
    # Change instance type.
    instance_type = rd.instance_type
    if instance_type == "db.r6g.large":
        instance_type = "db.r6g.xlarge"
    else:
        instance_type = "db.r6g.large"
    rd.rescale(immediate=True, new_paused=False, new_instance_type=instance_type)
    print(rd)
    # Pause.
    rd.rescale(immediate=True, new_paused=True)
    print(rd)
