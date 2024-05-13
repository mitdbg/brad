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
start_redshift_serverless_olap_runner 8 15 5 $ra_query_indexes "ra_8" $schema_name
rana_pid=$runner_pid

start_aurora_serverless_txn_runner_serial 4 $schema_name  # Implicit: --dataset-type
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# The workload should run for 90 minutes.
# We will run for ~100 mins to add some buffer.
sleep $(( 100 * 60 ))

# Shut down everything now.
log_workload_point "experiment_workload_done"
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"

