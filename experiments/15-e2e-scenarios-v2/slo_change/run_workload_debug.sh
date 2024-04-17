#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

export BRAD_IGNORE_BLUEPRINT=1
start_brad_debug $system_config_file $physical_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"
# 8 clients, offset 16 (for the transactional clients)
start_repeating_olap_runner 8 15 5 $ra_query_indexes "ra_8" 16
rana_pid=$runner_pid

start_txn_runner_serial 16  # Implicit: --dataset-type
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Sleep for 2 minutes and then change the SLOs.
sleep $(( 2 * 60 ))

log_workload_point "changing_slo"
brad cli --command "BRAD_CHANGE_SLO 30.0 0.030"
log_workload_point "changed_slo"

# Wait another 10 mins before stopping.
sleep $(( 10 * 60 ))

# Shut down everything now.
log_workload_point "experiment_workload_done"
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
