#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

start_brad $system_config_file $physical_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"
# 12 clients, offset 20 (for the transactional clients)
start_repeating_olap_runner 12 5 2 $ra_query_indexes "ra_8" 20
rana_pid=$runner_pid

start_txn_runner_serial 20  # Implicit: --dataset-type
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Sleep for 10 minutes and then change the SLOs.
sleep $(( 10 * 60 ))

log_workload_point "changing_slo"
brad cli --command "BRAD_CHANGE_SLO 30.0 0.015"
log_workload_point "changed_slo"

# Wait another hour before stopping.
sleep $(( 60 * 60 ))

# Shut down everything now.
log_workload_point "experiment_workload_done"
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
