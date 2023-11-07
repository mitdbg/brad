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

export BRAD_IGNORE_BLUEPRINT=1
start_brad $config_file $planner_config_file
sleep 30

# 1 Analytical runner
start_repeating_olap_runner 1 3 0 $ra_query_indexes "ra_1"
rana_pid=$runner_pid

# 1 Transaction runner
start_txn_runner 1 1
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Run for 10 minutes.
sleep $((60 * 10))

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
