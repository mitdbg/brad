#! /bin/bash

if [ -z $4 ]; then
  >&2 echo "Usage: $0 <config_path> <aurora instance id> <aurora cluster id> <rank (0 or 1)>"
  >&2 echo "The config path should be relative to the aurora/ subdirectory."
  exit 1
fi

export BRAD_CONFIG=$1
export BRAD_AURORA_INSTANCE_ID=$2
export BRAD_SCHEMA="imdb_extended_100g"

db_instance=$2
cluster_id=$3
rank=$4

function run_warm_up() {
  >&2 echo "Running warm up..."
  pushd aurora
  python3 -m brad.calibration.measure_load --run-warmup --engine aurora --query-file ../../../../workloads/IMDB_100GB/scaling_20/queries.sql
  popd
}

function modify_instance_sync() {
    local instance=$1
    local new_type=$2

    >&2 echo "Switching $instance to $new_type"
    aws rds modify-db-instance --db-instance-identifier $instance --db-instance-class $new_type --apply-immediately > /dev/null
    sleep 60

    while [[ "$(aws rds describe-db-instances --db-instance-identifier $instance --query 'DBInstances[0].DBInstanceStatus')" == "\"modifying\"" ]]; do
        >&2 echo "Waiting for the change to $instance to complete..."
        sleep 10
    done

    >&2 echo "Instance modified successfully."
}

all_instances=(
  "r6g_4xlarge"
  "x2g_4xlarge"
  "r6g_2xlarge"
  "x2g_2xlarge"
  "r6g_xlarge"
  "x2g_xlarge"
  "r6g_large"
  "x2g_large"
  "t4g_large"
  "t4g_medium"
)

>&2 echo "Starting as rank $rank"
>&2 echo "Running $BRAD_AURORA_INSTANCE_ID"
>&2 echo "Config $BRAD_CONFIG"
>&2 echo "Cluster id $cluster_id"
>&2 echo "Instance id $db_instance"
sleep 10

for inst_type in "${all_instances[@]}"; do
  aws_inst_id="${inst_type//_/.}"
  >&2 echo $aws_inst_id
  modify_instance_sync $db_instance "db.${aws_inst_id}"
  >&2 echo "Warming up..."
  run_warm_up
  >&2 echo "Running..."
  cond run //aurora/:${inst_type}-${rank}-of-2
done

sleep 60
>&2 echo "Done. Pausing $cluster_id"
aws rds stop-db-cluster --db-instance-identifier $cluster_id
