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

start_brad $config_file $planner_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"
start_repeating_olap_runner 1 30 5  # Implicit: --query-indexes
start_txn_runner 1
log_workload_point "clients_started"

# Wait until a re-plan and transition completes
poll_file_for_event $COND_OUT/brad_daemon_events.csv "post_transition_completed" 15
log_workload_point "after_replan"

# Wait 5 minutes before shutting down (to get steady state performance)
sleep 300
log_workload_point "experiment_workload_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown
log_workload_point "shutdown_complete"
