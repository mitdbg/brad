#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Used to set up BRAD for this experiment.

# Arguments:
# --config-file
# --schema-name
extract_named_arguments $@

# This will block until the transition completes, assuming the current blueprint
# is in a "good" state.
brad admin modify_blueprint \
  --config-file $config_file \
  --schema-name $schema_name \
  --aurora-instance-type db.r6g.xlarge \
  --aurora-num-nodes 1 \
  --redshift-instance-type dc2.large \
  --redshift-num-nodes 2 \
  --place-tables-everywhere
