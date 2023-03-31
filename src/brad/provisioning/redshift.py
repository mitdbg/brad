import boto3
import time
import logging
import os

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

class RedshiftProvisioning:
    # Initialize provisioning.
    # If cluster does not exist, you must specify its initial state for it to be created.
    def __init__(self, cluster_name="brad-cluster0", initial_instance_type=None, initial_cluster_size=None):
        self.cluster_name = cluster_name
        self.instance_type = initial_instance_type
        self.cluster_size = initial_cluster_size
        self.paused = None
        self.address = None
        self.port = None
        self.redshift = boto3.client("redshift")
        self.redeploy()

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
        if new_cluster_size is not None:
            if new_cluster_size == self.cluster_size or new_cluster_size == self.cluster_size / 2 or new_cluster_size == self.cluster_size*2:
                self.cluster_size = new_cluster_size
            else:
                raise RuntimeError("Passed in impossible cluster size. Check Reshift Docs.")
        if new_paused is not None:
            self.paused = new_paused
        self.redeploy()

    # Redeploy. Used for initialization and rescaling. 
    def redeploy(self):
        # Iterate until cluster is in right state.
        while True:
            try:
                # Read cluster information.
                response = self.redshift.describe_clusters(
                    ClusterIdentifier=self.cluster_name,
                )
                logging.debug(f"Cluster state: {response}")
                cluster = response["Clusters"][0]
                modifying = cluster["ClusterAvailabilityStatus"] == "Modifying"
                status = cluster["ClusterStatus"]
                instance_type = cluster["NodeType"]
                cluster_size = cluster["NumberOfNodes"]
                endpoint = cluster["Endpoint"]
                
                # Set default values.
                self.address =  endpoint["Address"]
                self.port = endpoint["Port"]
                if self.paused is None:
                    self.paused = status in ("paused", "pausing")
                if self.instance_type is None:
                    self.instance_type = instance_type
                if self.cluster_size is None:
                    self.cluster_size = cluster_size
                
                # Reconcile expected status and current status.
                if modifying:
                    # Wait for status to change.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Status: {status}. Is Modifying. Waiting...")
                    time.sleep(5.0)
                    continue
                if status == "paused" and self.paused:
                    # Nothing to do.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Is in right state.")
                    return
                if self.paused and status == "available":
                    # Should pause.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Pausing...")
                    self.redshift.pause_cluster(
                        ClusterIdentifier=self.cluster_name
                    )
                    time.sleep(5.0)
                    continue
                if not(self.paused) and status == "paused":
                    # Should start.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Restarting...")
                    self.redshift.resume_cluster(
                        ClusterIdentifier=self.cluster_name
                    )
                    time.sleep(5.0)
                    continue
                # Reconcile expected instance type and count with current ones.
                if instance_type == self.instance_type and cluster_size == self.cluster_size:
                    # Cluster is as it should be. Return.
                    logging.info(f"Redshift Cluster {self.cluster_name}. Is in right state.")
                    return
                # Must resize cluster.
                logging.info(f"Redshift Cluster {self.cluster_name}. Resizing to ({self.instance_type}, {self.cluster_size})...")
                self.redshift.resize_cluster(
                    ClusterIdentifier=self.cluster_name,
                    NodeType=self.instance_type,
                    NumberOfNodes=self.cluster_size,
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
                        MasterUsername='brad',
                        MasterUserPassword='BradBrad123',
                        NumberOfNodes=self.cluster_size,
                        PubliclyAccessible=True,
                    )
                else:
                    raise e

if __name__ == "__main__":
    # Get or create cluster.
    try:
        rd = RedshiftProvisioning(cluster_name="brad-cluster0")
    except:
        rd = RedshiftProvisioning(cluster_name="brad-cluster0", initial_instance_type="ra3.xlplus", initial_cluster_size=2)
    # Change cluster size.
    cluster_size = rd.cluster_size
    if cluster_size == 2:
        cluster_size = 4
    else:
        cluster_size = 2
    rd.rescale(new_cluster_size=cluster_size, new_paused=False)
    print(rd)
    # Pause.
    rd.rescale(new_paused=True)
    print(rd)
