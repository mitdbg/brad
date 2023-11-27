#! /bin/bash

EXPT_OUT="expt_out"
ANALYTICS_ENGINE="tidb"
TRANSACTION_ENGINE="tidb"
time_scale_factor=24
gap_dist_path="workloads/IMDB_20GB/regular_test/gap_time_dist.npy"
query_frequency_path="workloads/IMDB_100GB/regular_test/query_frequency.npy"
num_client_path="workloads/IMDB_20GB/regular_test/num_client.pkl"
total_second_phase_time_s=3600
script_loc=$(cd $(dirname $0) && pwd -P)
source $script_loc/../common_daylong.sh

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
echo "Query Bank"
# Check file and exit if not exists
ls $ra_query_bank_file || exit 1
ls $gap_dist_path || exit 1
ls $query_frequency_path || exit 1
ls $num_client_path || exit 1


log_workload_point "clients_starting"
start_repeating_olap_runner 10 15 5 $ra_query_indexes "ra_daylong"
rana_pid=$runner_pid

start_txn_runner 4
txn_pid=$runner_pid


# start_repeating_olap_runner 10 70 5 "61,71,75" "ra_1_special"
# rana2_pid=$runner_pid
# log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Wait until a re-plan and transition completes.
# Expected:
# - Downscale Aurora
# - Turn off Redshift
# Detection time is ~5 minutes
# Transition time is ~7 minutes
wait_start="$(date -u +%s)"


wait_end="$(date -u +%s)"
transition_elapsed_s="$(($wait_end - $wait_start))"

if (( $transition_elapsed_s < $total_second_phase_time_s )); then
  # Pause for a "standard" amount of time. We want to do this to be able to
  # align the baseline measured metrics against our metrics.
  leftover_s=$(($total_second_phase_time_s - $transition_elapsed_s))
  >&2 echo "Waiting $leftover_s seconds before stopping..."
  sleep $leftover_s
fi
log_workload_point "experiment_workload_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
