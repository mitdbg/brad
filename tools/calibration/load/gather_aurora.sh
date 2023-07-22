#! /bin/bash

if [ -z $1 ]; then
  >&2 echo "Usage: $0 <config_path>"
  >&2 echo "The config path should be relative to the aurora/ subdirectory."
  exit 1
fi

# N.B. Both "imdb" and "imdb_extended" should be fine.
export BRAD_SCHEMA="imdb_extended"
export BRAD_CONFIG=$1

function run_warm_up() {
  >&2 echo "Running warm up..."
  pushd aurora
  python3 -m brad.calibration.measure_load --run-warmup --engine aurora --query-file ../query_bank.sql
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

# db.r6g.2xlarge
>&2 echo "r6g.2xlarge"
modify_instance_sync $db_instance "db.r6g.2xlarge"
>&2 echo "Warming up..."
run_warm_up
cond run //aurora/:r6g_2xlarge

# db.r6g.xlarge
>&2 echo "r6g.xlarge"
modify_instance_sync $db_instance "db.r6g.xlarge"
>&2 echo "Warming up..."
run_warm_up
cond run //aurora/:r6g_xlarge

# db.r6g.large
>&2 echo "r6g.large"
modify_instance_sync $db_instance "db.r6g.large"
>&2 echo "Warming up..."
run_warm_up
cond run //aurora/:r6g_large
