#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

schema_name="imdb_extended_100g"

log_workload_point "clients_starting"
# 12 clients, offset 20 (for the transactional clients)
start_redshift_serverless_olap_runner 12 5 2 $ra_query_indexes "ra_8" $schema_name
rana_pid=$runner_pid

start_aurora_serverless_txn_runner_serial 20 $schema_name  # Implicit: --dataset-type
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Sleep for 10 minutes and then change the SLOs.
sleep $(( 10 * 60 ))

# No-op (changing SLOs on BRAD).

# Wait another hour before stopping.
sleep $(( 60 * 60 ))

# Shut down everything now.
log_workload_point "experiment_workload_done"
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"

