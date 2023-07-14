import boto3
import time
import logging
import os

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class RedshiftProvisioning:
    # Initialize provisioning.
    # If cluster does not exist, you must specify its initial state for it to be created.
    def __init__(
        self,
        cluster_name="brad-cluster0",
        initial_instance_type=None,
        initial_cluster_size=None,
        always_classic=False,
    ):
        self.cluster_name = cluster_name
        self.instance_type = initial_instance_type
        self.cluster_size = initial_cluster_size
        self.paused = None
        self.address = None
        self.port = None
        self.always_classic = always_classic
        self.redshift = boto3.client("redshift")
        self.redeploy(False)

    # String representation.
    def __str__(self) -> str:
        return f"RedshiftCluster(name={self.cluster_name}, instance_type={self.instance_type}, size={self.cluster_size}, paused={self.paused}, address={self.address}, port={self.port})"

    # Delete cluster.
    def delete_cluster(self):
        self.redshift.delete_cluster(
            ClusterIdentifier=self.cluster_name,
            SkipFinalClusterSnapshot=True,
        )

    # Return connection information.
    def connection_info(self):
        return (self.address, self.port)

    # Rescale a cluster.
    # This will wait until new cluster's state is avalaible.
    # Start it in a new thread if you don't want to wait.
    def rescale(self, new_instance_type=None, new_cluster_size=None, new_paused=None):
        if new_instance_type is not None:
            self.instance_type = new_instance_type
        old_cluster_size = self.cluster_size
        if new_cluster_size is not None:
            self.cluster_size = new_cluster_size
        if new_paused is not None:
            self.paused = new_paused
        is_classic = self.get_resize_type(old_cluster_size)
        self.redeploy(is_classic=is_classic)

    # Resize depends on target instance type, and old instance count, and the new instance count.
    def get_resize_type(self, old_cluster_size) -> bool:
        if self.always_classic:
            return True
        if old_cluster_size == 1 or self.cluster_size == 1:
            return True
        if self.instance_type in ["ra3.16xlarge", "ra3.4xlarge"]:
            return (
                self.cluster_size < old_cluster_size / 4
                or self.cluster_size > 4 * old_cluster_size
            )
        if self.instance_type in ["ra3.xlplus"]:
            return (
                self.cluster_size < old_cluster_size / 4
                or self.cluster_size > 2 * old_cluster_size
            )
        # For all other types, must be within double or half.
        return (
            self.cluster_size < old_cluster_size / 2
            or self.cluster_size > 2 * old_cluster_size
        )

    # Redeploy. Used for initialization and rescaling.
    def redeploy(self, is_classic: bool):
        # Iterate until cluster is in right state.
        while True:
            try:
                # Read cluster information.
                response = self.redshift.describe_clusters(
                    ClusterIdentifier=self.cluster_name,
                )
                logging.info(f"Cluster state: {response}")
                cluster = response["Clusters"][0]
                modifying = cluster["ClusterAvailabilityStatus"] == "Modifying"
                status = cluster["ClusterStatus"]
                instance_type = cluster["NodeType"]
                curr_cluster_size = cluster["NumberOfNodes"]
                if "Endpoint" not in cluster:
                    logging.info("Endpoint not yet set")
                    time.sleep(5.0)
                    continue
                endpoint = cluster["Endpoint"]

                # Set default values.
                self.address = endpoint["Address"]
                self.port = endpoint["Port"]
                if self.paused is None:
                    self.paused = status in ("paused", "pausing")
                if self.instance_type is None:
                    self.instance_type = instance_type
                if self.cluster_size is None:
                    self.cluster_size = curr_cluster_size

                # Reconcile expected status and current status.
                if modifying:
                    # Wait for status to change.
                    logging.info(
                        f"Redshift Cluster {self.cluster_name}. Status: {status}. Is Modifying. Waiting..."
                    )
                    time.sleep(5.0)
                    continue
                if status == "paused" and self.paused:
                    # Nothing to do.
                    logging.info(
                        f"Redshift Cluster {self.cluster_name}. Is in right state."
                    )
                    return
                if self.paused and status == "available":
                    # Should pause.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Pausing...")
                    self.redshift.pause_cluster(ClusterIdentifier=self.cluster_name)
                    time.sleep(5.0)
                    continue
                if not (self.paused) and status == "paused":
                    # Should start.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Restarting...")
                    self.redshift.resume_cluster(ClusterIdentifier=self.cluster_name)
                    time.sleep(5.0)
                    continue
                # Reconcile expected instance type and count with current ones.
                if (
                    instance_type == self.instance_type
                    and curr_cluster_size == self.cluster_size
                ):
                    # Cluster is as it should be. Return.
                    logging.info(
                        f"Redshift Cluster {self.cluster_name}. Is in right state."
                    )
                    return
                # Must resize cluster.
                logging.info(
                    f"Redshift Cluster {self.cluster_name}. Resizing to ({self.instance_type}, {self.cluster_size})..."
                )
                print(f"IsClassic: {is_classic}")
                if is_classic:
                    cluster_type = (
                        "multi-node" if self.cluster_size > 1 else "single-node"
                    )
                    self.redshift.modify_cluster(
                        ClusterIdentifier=self.cluster_name,
                        ClusterType=cluster_type,
                        NodeType=self.instance_type,
                        NumberOfNodes=self.cluster_size,
                    )
                else:
                    self.redshift.resize_cluster(
                        ClusterIdentifier=self.cluster_name,
                        NodeType=self.instance_type,
                        NumberOfNodes=self.cluster_size,
                        Classic=is_classic,
                    )
                # Next iteration of the loop will wait for availability.
                time.sleep(5.0)
                continue
            except Exception as e:
                e_str = f"{e}"
                if "NotFound" in e_str:
                    # Create First.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Creating...")
                    self.redshift.create_cluster(
                        ClusterIdentifier=self.cluster_name,
                        NodeType=self.instance_type,
                        MasterUsername="brad",
                        MasterUserPassword="BradBrad123",
                        NumberOfNodes=self.cluster_size,
                        PubliclyAccessible=True,
                    )
                else:
                    print(f"RERAISING BRAD ERROR: {e}")
                    raise e


if __name__ == "__main__":
    # Get or create cluster.
    start_time = time.time()
    try:
        rd = RedshiftProvisioning(cluster_name="brad-cluster0")
    except Exception as _e:
        rd = RedshiftProvisioning(
            cluster_name="brad-cluster0",
            initial_instance_type="ra3.xlplus",
            initial_cluster_size=1,
            always_classic=False,
        )
    end_time = time.time()
    create_duration = start_time - end_time
    # Change cluster size.
    cluster_size = rd.cluster_size
    if cluster_size == 1:
        cluster_size = 2
    else:
        cluster_size = 1
    start_time = time.time()
    rd.rescale(new_cluster_size=cluster_size, new_paused=False)
    end_time = time.time()
    rescale_duration = start_time - end_time
    print(rd)
    # Pause.
    start_time = time.time()
    rd.rescale(new_paused=True)
    end_time = time.time()
    pause_duration = start_time - end_time
    print(rd)
    print(
        f"CreateDur={create_duration:.2f}. RescaleDur={rescale_duration:.2f}. PauseDur={pause_duration:.2f}"
    )
