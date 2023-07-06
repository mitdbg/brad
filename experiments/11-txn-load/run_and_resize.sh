#! /bin/bash

db_instance="aurora-secondary-cluster"

function modify_instance_sync() {
    local instance=$1
    local new_type=$2

    >&2 echo "Switching $instance to $new_type"
    aws rds modify-db-instance --db-instance-identifier $instance --db-instance-class $new_type --apply-immediately > /dev/null
    sleep 20

    while [[ "$(aws rds describe-db-instances --db-instance-identifier $instance --query 'DBInstances[0].DBInstanceStatus')" == "\"modifying\"" ]]; do
        >&2 echo "Waiting for the change to $instance to complete..."
        sleep 10
    done

    >&2 echo "Instance modified successfully."
}

# db.r6g.large
>&2 echo "r6g.large"
modify_instance_sync $db_instance "db.r6g.large"
>&2 echo "Trimming..."
python3 runner.py --cstr_var AURORA_CSTR --run_trim
>&2 echo "Warming up..."
python3 runner.py --cstr_var AURORA_CSTR --run_warmup
cond run 11-txn-load/:r6g_large

# db.r6g.xlarge
>&2 echo "r6g.xlarge"
modify_instance_sync $db_instance "db.r6g.xlarge"
>&2 echo "Trimming..."
python3 runner.py --cstr_var AURORA_CSTR --run_trim
>&2 echo "Warming up..."
python3 runner.py --cstr_var AURORA_CSTR --run_warmup
cond run 11-txn-load/:r6g_xlarge

# db.r6g.2xlarge
>&2 echo "r6g.2xlarge"
modify_instance_sync $db_instance "db.r6g.2xlarge"
>&2 echo "Trimming..."
python3 runner.py --cstr_var AURORA_CSTR --run_trim
>&2 echo "Warming up..."
python3 runner.py --cstr_var AURORA_CSTR --run_warmup
cond run 11-txn-load/:r6g_2xlarge
