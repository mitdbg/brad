#! /bin/bash

ANALYTICS_ENGINE="redshift"
TRANSACTION_ENGINE="aurora"
EXPT_OUT="expt_out_daylong_${ANALYTICS_ENGINE}_${TRANSACTION_ENGINE}"
mkdir -p $EXPT_OUT
hours=12
time_scale_factor=$(bc <<< "scale=0; 24 / $hours")
run_for_s=$(bc <<< "scale=0; ($hours * 60 * 60) / 1.0")
# Add 5 minutes of buffer time.
total_time_s=$(($run_for_s + 5 * 60))
num_client_path="workloads/IMDB_20GB/regular_test/num_client.pkl"
clients_multiplier=1
analytics_issue_slots=20
txn_issue_slots=1
max_num_clients=$((10 * $clients_multiplier))
script_loc=$(cd $(dirname $0) && pwd -P)
source $script_loc/../common_daylong.sh
trace_manifest=$script_loc/trace_manifest.yml

# TODO: This executor file should be adapted to run against the baselines too
# (TiDB / Serverless Redshift + Aurora)

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

# Check file and exit if not exists
ls $trace_manifest || exit 1

log_workload_point "clients_starting"

# Analytics.
start_trace_runner
rana_pid=$runner_pid

# Transactions.
start_trace_txn_runner $max_num_clients $time_scale_factor $clients_multiplier "t" $run_for_s
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $adhoc_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

sleep $total_time_s
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid $adhoc_pid
log_workload_point "shutdown_complete"
