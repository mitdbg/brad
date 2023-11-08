#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Scenario:
# - Start from 2x t4g.medium Aurora, Redshift off
# - 4 T clients, increasing to 24 T clients by 4 every minute
# We expect BRAD to scale up Aurora at this point, but not start Redshift (a replica should be fine for the analytical workload)
# - Increase the analytical load + add new "heavier" queries - expect that these go to Athena
# - Increase frequency of queries, expect Redshift to start (go straight to 2x dc2.large to avoid classic resize for practical purposes)

# TODO: This executor file should be adapted to run against the baselines too
# (TiDB / Serverless Redshift + Aurora)

function step_txns() {
  local lo=$1
  local hi=$2
  local gap_minute=$3
}

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

function txn_sweep() {
  local sweep=$1
  local gap_minute=$2
  local keep_last=$3

  for t_clients in $sweep; do
    start_txn_runner $t_clients  # Implicit: --dataset-type
    txn_pid=$runner_pid

    sleep $(($gap_minute * 60))
    if [[ ! -z $keep_last ]] && [[ $t_clients = $keep_last ]]; then
      kill -INT $txn_pid
      wait $txn_pid
    fi
  done
}

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

start_brad $config_file $planner_config_file
log_workload_point "brad_start_initiated"
sleep 30

# Start with 8 analytical clients.
log_workload_point "ra_client_starting"
start_repeating_olap_runner 8 15 5 $ra_query_indexes "ra_8"
rana_pid=$runner_pid

# Scale up to 8 transactional clients and hold for 15 minutes.
txn_sweep "4 5 6 7 8" 1 8
sleep $((15 * 60))

# Scale up to 24 transactional clients.
kill -INT $txn_pid
wait $txn_pid
txn_sweep "12 16 20 24" 2 24

# 5 minutes
kill -INT $rana_pid
wait $rana_pid
start_repeating_olap_runner 16 15 5 $ra_query_indexes "ra_16"
rana_pid=$runner_pid
sleep $((5 * 60))

# 20 minutes
kill -INT $rana_pid
wait $rana_pid
start_repeating_olap_runner 24 15 5 $ra_query_indexes "ra_24"
rana_pid=$runner_pid
sleep $((20 * 60))
log_workload_point "experiment_workload_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
