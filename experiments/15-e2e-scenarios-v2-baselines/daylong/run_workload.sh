#! /bin/bash

EXPT_OUT="expt_out"
ANALYTICS_ENGINE="redshift"
TRANSACTION_ENGINE="aurora"
hours=12
time_scale_factor=$((24 / $hours))
total_time_s=$(($hours * 60 * 60 + 5 * 60))
clients_multiplier=1
gap_dist_path="workloads/IMDB_20GB/regular_test/gap_time_dist.npy"
query_frequency_path="workloads/IMDB_100GB/regular_test/query_frequency.npy"
num_client_path="workloads/IMDB_20GB/regular_test/num_client.pkl"
seq_query_bank_file="workloads/IMDB_100GB/adhoc_test/queries.sql"
script_loc=$(cd $(dirname $0) && pwd -P)
source $script_loc/../common_daylong.sh

# TODO: This executor file should be adapted to run against the baselines too
# (TiDB / Serverless Redshift + Aurora)

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

# Check file and exit if not exists
ls $ra_query_bank_file || exit 1
ls $gap_dist_path || exit 1
ls $query_frequency_path || exit 1
ls $num_client_path || exit 1
ls $seq_query_bank_file || exit 1

log_workload_point "clients_starting"

# Repeating analytics.
start_snowset_repeating_olap_runner $((10 * $clients_multiplier)) $time_scale_factor $clients_multiplier "ra"
rana_pid=$runner_pid

# Transactions.
start_snowset_txn_runner $((10 * $clients_multiplier)) $time_scale_factor $clients_multiplier "t"
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

sleep $total_time_s
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid $adhoc_pid
log_workload_point "shutdown_complete"
