#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# TODO: This executor file should be adapted to run against the baselines too
# (TiDB / Serverless Redshift + Aurora)

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

trap "cancel_experiment" INT
trap "cancel_experiment" TERM

# Useful for testing out blueprint planning without executing the transition.
export BRAD_IGNORE_BLUEPRINT=1
start_brad_debug $config_file $planner_config_file
sleep 10

start_repeating_olap_runner 2 30 5  # Implicit: --query-indexes
start_txn_runner 4

echo "READY -- Sleeping for 1 hour. Hit Ctrl-C to stop."
sleep $((60 * 60))
