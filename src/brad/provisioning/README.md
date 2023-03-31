# Provisioning
* Allows provisioning Redshift and Aurora.
* Allows simple scaling decision. Currently Supported.
    * For booting Redshift: Based on data scanned by Athena, check if Redshift should be started.
    * For scaling up Redshift: Find percentage of Redshift queries above a given threshold.
    * For scaling up Aurora: Check if aurora's average DML or Select query exceeds a given threshold.
        * TODO: Find percentage of Aurora queries above a given threshold. Requires special setup.
    * For scaling up or down Redshift or Aurora: check if cpu, memory, or disk utilization of Aurora or Redshift are over or under a threshold.
    * **Autoscaling is untested so far.**
* Current Limitations:
    * Aurora's provisioner does not yet support read-replicas or online rescaling.
    * TODO: What else?
* NOTE: Changes can be slow. Launch a new thread when calling the `rescale` functions.

## Redshift Usage
```py
# Get cluster if already created. Otherwise create cluster.
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
# Show connection info.
print(rd.connection_info())
```

## Aurora Usage
```py
# Get or create cluster.
try:
    rd = RdsProvisioning(cluster_name="brad-cluster0")
except:
    rd = RdsProvisioning(cluster_name="brad-cluster0", initial_instance_type="db.r6g.large")
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
# Show connection info.
print(rd.connection_info())
```


## Autoscaling Information
```py
ps = ProvisionScaling(athena_catalog_name="bradcatalog0", redshift_cluster_name="brad-cluster0", aurora_cluster_name="brad-cluster0")
# Each function takes window over which to check past utilization.

# Check if it is economic to start the given redshift cluster.
ps.can_boot_redshift(node_type="ra3.xlplus", node_count=2, window_minutes=10)
# Check if Redshift's CPU is over used.
ps.check_redshift_utilization(check_overused=True, utilization_type="cpu", utilization_threshold=90, window_minutes=10)
# Check if Aurora's memory is under used.
ps.check_aurora_utilization(check_overused=False, utilization_type="memory", utilization_threshold=40, window_minutes=10, node_type="db.r6g.large")
# Find percentage of slow redshift queries.
ps.get_redshift_slow_percentage(running_time_threshold_secs=10, window_minutes=10)
# Check aurora's average latency has exceed a threshold.
ps.is_aurora_slow(latency_threshold_ms=500, window_minutes=10)
```