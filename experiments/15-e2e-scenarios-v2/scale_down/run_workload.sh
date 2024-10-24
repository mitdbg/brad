#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# TODO: This executor file should be adapted to run against the baselines too
# (TiDB / Serverless Redshift + Aurora)

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

start_brad $system_config_file $physical_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"
start_repeating_olap_runner 8 15 5 $ra_query_indexes "ra_8"
rana_pid=$runner_pid

start_txn_runner_serial 4  # Implicit: --dataset-type
txn_pid=$runner_pid

log_workload_point "clients_started"

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
# Allow the workload to run for 1.5 hours.
total_second_phase_time_s="$((90 * 60))"
wait_start="$(date -u +%s)"

poll_file_for_event $COND_OUT/brad_daemon_events.csv "post_transition_completed" 30
log_workload_point "after_replan"

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
