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

start_brad $config_file $planner_config_file
sleep 30

start_repeating_olap_runner 1 30 5  # Implicit: --query-indexes
start_txn_runner 2

echo "READY -- Sleeping for 1 hour. Hit Ctrl-C to stop."
sleep $((60 * 60))