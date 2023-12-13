#! /bin/bash

EXPT_OUT="expt_out"
ANALYTICS_ENGINE="redshift"
TRANSACTION_ENGINE="aurora"
VECTOR_ENGINE="aurora"
script_loc=$(cd $(dirname $0) && pwd -P)
total_time_s=7200 # 2 hours
source $script_loc/../common.sh

# TODO: This executor file should be adapted to run against the baselines too
# (TiDB / Serverless Redshift + Aurora)

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

# Just fo testing.
echo "Query indexes"
echo $ra_query_indexes
echo "rana Query Bank"
# Check file and exit if not exists
ls $ra_query_bank_file || exit 1
echo "Vector Query Bank"
ls $vector_query_bank_file || exit 1

query_indices="62,64,65,66,69,72,73,74,91,59"
heavier_queries="14,54,60,71,75"
all_queries="${query_indices},${heavier_queries}"

start_repeating_olap_runner 8 5 5 $all_queries "ra_8"
rana_pid=$runner_pid

start_repeating_olap_runner 2 8 5 "" "ra_vector"
other_pid=$runner_pid

start_txn_runner 4
txn_pid=$runner_pid
log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $other_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

sleep $total_time_s  # Wait for 2 hours.
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid $other_pid
log_workload_point "shutdown_complete"
