#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

# Repeating query indexes:
# 51, 53, 58, 61, 62, 64, 65, 66, 69, 72, 73, 74, 77, 86, 91
#
# Touch `title`:
# 65, 69, 73
#
# Heavy repeating query indexes:
# 14, 54, 59, 60, 71, 75
#
# Touch `title`:
# 14, 54, 59, 75

query_indices="62,64,65,66,69,72,73,74,91,59"
heavier_queries="14,54,60,71,75"

export BRAD_IGNORE_BLUEPRINT=1
start_brad_debug $system_config_file $physical_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"
start_repeating_olap_runner 8 5 5 $query_indices "ra_8"
rana_pid=$runner_pid

start_other_repeating_runner 2 8 5 "ra_vector" 8
other_pid=$runner_pid

start_txn_runner 4  # Implicit: --dataset-type
txn_pid=$runner_pid

start_repeating_olap_runner 4 5 5 $heavier_queries "ra_4_heavy" 10
heavy_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $other_pid $heavy_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

sleep $((60 * 60))  # Wait for 1 hour.
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid $other_pid $heavy_pid
log_workload_point "shutdown_complete"
