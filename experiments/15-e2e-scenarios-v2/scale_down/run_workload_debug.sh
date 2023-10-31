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

start_repeating_olap_runner 8 15 5 $ra_query_indexes
rana_pid=$runner_pid

start_txn_runner 8
txn_pid=$runner_pid

start_repeating_olap_runner 1 70 5 "60,61,71,75"
rana2_pid=$runner_pid

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $rana2_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

echo "READY -- Sleeping for 1 hour. Hit Ctrl-C to stop."
sleep $((60 * 60))
inner_cancel_experiment
