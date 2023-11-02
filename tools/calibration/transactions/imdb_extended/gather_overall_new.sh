#! /bin/bash

if [ -z $2 ]; then
  >&2 echo "Usage: $0 <config file> <schema name>"
  exit 1
fi

config_file=$1
schema_name=$2

export BRAD_CONFIG_FILE=$config_file

function modify_instance_sync() {
    local new_instance=$1

    >&2 echo "Ensuring Aurora is $new_instance"

    # N.B. This modification script is blocking.
    brad admin --debug modify_blueprint \
        --schema-name $schema_name \
        --config-file $config_file \
        --aurora-num-nodes 1 \
        --aurora-instance-type $new_instance

    >&2 echo "Instance modified successfully."
}

# Start the cluster first.
brad admin --debug control resume --schema-name $schema_name --config-file $config_file

# db.r6g.4xlarge
>&2 echo "r6g.4xlarge"
modify_instance_sync "db.r6g.4xlarge"
cond run //imdb_extended/:r6g_4xlarge-$schema_name

# db.r6g.2xlarge
>&2 echo "r6g.2xlarge"
modify_instance_sync "db.r6g.2xlarge"
cond run //imdb_extended/:r6g_2xlarge-$schema_name

# db.r6g.xlarge
>&2 echo "r6g.xlarge"
modify_instance_sync "db.r6g.xlarge"
cond run //imdb_extended/:r6g_xlarge-$schema_name

# db.r6g.large
>&2 echo "r6g.large"
modify_instance_sync "db.r6g.large"
cond run //imdb_extended/:r6g_large-$schema_name

# db.t4g.medium
>&2 echo "t4g.medium"
modify_instance_sync "db.t4g.medium"
cond run //imdb_extended/:t4g_medium-$schema_name

brad admin --debug control pause --schema-name $schema_name --config-file $config_file
