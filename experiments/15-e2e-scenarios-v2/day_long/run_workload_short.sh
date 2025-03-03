#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

start_brad $system_config_file $physical_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"

clients_multiplier=1
time_scale_factor=12  # Runs for 2 hours
run_for_s=$((2 * 60 * 60))

# Repeating analytics.
start_snowset_repeating_olap_runner $((10 * $clients_multiplier)) $time_scale_factor $clients_multiplier "ra" $run_for_s
rana_pid=$runner_pid

# Transactions.
start_snowset_txn_runner $((10 * $clients_multiplier)) $time_scale_factor $clients_multiplier "t" $run_for_s
txn_pid=$runner_pid

# Ad-hoc queries.
# 2 clients, issuing once per 8 minutes on average with a standard deviation of
# 2 minutes.
start_sequence_runner 2 $((8 * 60)) $((2 * 60)) "adhoc"
adhoc_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $adhoc_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

sleep $run_for_s
sleep $((5 * 60))  # Wait an extra 5 minutes.
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid $adhoc_pid
log_workload_point "shutdown_complete"
