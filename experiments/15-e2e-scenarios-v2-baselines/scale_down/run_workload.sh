#! /bin/bash

EXPT_OUT="expt_out"
ANALYTICS_ENGINE="tidb"
TRANSACTION_ENGINE="tidb"
script_loc=$(cd $(dirname $0) && pwd -P)
total_second_phase_time_s=$(( 90 * 60 ))
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
echo "Seq Query Bank"
ls $seq_query_bank_file || exit 1

# # Start seq runner
# start_seq_olap_runner 1 30 5 "downseq"
# seq_pid=$runner_pid


log_workload_point "clients_starting"
start_repeating_olap_runner 8 15 5 $ra_query_indexes "ra_8"
rana_pid=$runner_pid

start_txn_runner_serial 4
txn_pid=$runner_pid


function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $rana2_pid
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
graceful_shutdown $rana_pid $txn_pid $rana2_pid
log_workload_point "shutdown_complete"
