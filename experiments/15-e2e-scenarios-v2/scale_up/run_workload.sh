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

initial_queries="99,56,32,92,91,49,30,83,94,38,87,86,76,37,31,46"
heavier_queries="58,61,62,64,69"

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
log_workload_point "start_rana_8"
start_repeating_olap_runner 8 15 5 $initial_queries "ra_8"
rana_pid=$runner_pid

# Scale up to 8 transactional clients and hold for 15 minutes.
log_workload_point "start_increase_txn_4_to_8"
txn_sweep "4 5 6 7 8" 1 8
log_workload_point "hold_txn_8_15_min"
sleep $((15 * 60))

# Scale up to 28 transactional clients. Hold for 15 minutes.
log_workload_point "start_increase_txn_12_to_28"
kill -INT $txn_pid
wait $txn_pid
txn_sweep "12 16 20 24 28" 2 28
log_workload_point "hold_txn_28_15_min"
sleep $((15 * 60)) 

# 15 minutes.
log_workload_point "start_heavy_rana_8"
start_repeating_olap_runner 8 15 5 $heavier_queries "ra_8_heavy" 8
heavy_rana_pid=$runner_pid
sleep $((15 * 60))

# 20 minutes.
log_workload_point "start_heavy_rana_20"
kill -INT $heavy_rana_pid
wait $heavy_rana_pid
start_repeating_olap_runner 20 15 5 $heavier_queries "ra_20_heavy" 8
heavy_rana_pid=$runner_pid
sleep $((20 * 60))
log_workload_point "experiment_workload_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $heavy_rana_pid $txn_pid
log_workload_point "shutdown_complete"
