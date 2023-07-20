#! /bin/bash

if [ -z $2 ]; then
  >&2 echo "Usage: $0 <instance identifier> <cstr var>"
  exit 1
fi

instance_identifier=$1
cstr_var=$2

export BRAD_INSTANCE_ID=$instance_identifier
export BRAD_CSTR_VAR=$cstr_var

function modify_instance_sync() {
    local instance=$1
    local new_type=$2

    >&2 echo "Switching $instance to $new_type"
    aws rds modify-db-instance --db-instance-identifier $instance --db-instance-class $new_type --apply-immediately > /dev/null
    sleep 45

    while [[ "$(aws rds describe-db-instances --db-instance-identifier $instance --query 'DBInstances[0].DBInstanceStatus')" == "\"modifying\"" ]]; do
        >&2 echo "Waiting for the change to $instance to complete..."
        sleep 10
    done

    >&2 echo "Instance modified successfully."
}

# db.r6g.4xlarge
>&2 echo "r6g.4xlarge"
modify_instance_sync $instance_identifier "db.r6g.4xlarge"
cond run //imdb_extended/:r6g_4xlarge

# db.r6g.2xlarge
>&2 echo "r6g.2xlarge"
modify_instance_sync $instance_identifier "db.r6g.2xlarge"
cond run //imdb_extended/:r6g_2xlarge

# db.r6g.xlarge
>&2 echo "r6g.xlarge"
modify_instance_sync $instance_identifier "db.r6g.xlarge"
cond run //imdb_extended/:r6g_xlarge

# db.r6g.large
>&2 echo "r6g.large"
modify_instance_sync $instance_identifier "db.r6g.large"
cond run //imdb_extended/:r6g_large
