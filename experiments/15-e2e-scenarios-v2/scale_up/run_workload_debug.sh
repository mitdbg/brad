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
heavier_queries="58,61,62,64,69,70,71,72,73,74"

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

export BRAD_IGNORE_BLUEPRINT=1
start_brad_debug $config_file $planner_config_file
log_workload_point "brad_start_initiated"
sleep 30

function point_one() {
  # A: 8x
  # T: 4x
  local run_for_minutes=$1
  start_repeating_olap_runner 8 15 5 $initial_queries "ra_8"
  rana_pid=$runner_pid
  start_txn_runner 4  # Implicit: --dataset-type
  txn_pid=$runner_pid

  sleep $(($run_for_minutes * 60))
  kill -INT $rana_pid
  kill -INT $txn_pid
  wait $rana_pid
  wait $txn_pid
}

function point_two() {
  # A: 8x
  # T: 8x
  local run_for_minutes=$1
  start_repeating_olap_runner 8 15 5 $initial_queries "ra_8"
  rana_pid=$runner_pid
  start_txn_runner 8  # Implicit: --dataset-type
  txn_pid=$runner_pid

  sleep $(($run_for_minutes * 60))
  kill -INT $rana_pid
  kill -INT $txn_pid
  wait $rana_pid
  wait $txn_pid
}

function point_three() {
  # A: 8x
  # T: 28x
  local run_for_minutes=$1
  start_repeating_olap_runner 8 15 5 $initial_queries "ra_8"
  rana_pid=$runner_pid
  start_txn_runner 28  # Implicit: --dataset-type
  txn_pid=$runner_pid

  sleep $(($run_for_minutes * 60))
  kill -INT $rana_pid
  kill -INT $txn_pid
  wait $rana_pid
  wait $txn_pid
}

function point_four() {
  # A: 16x
  # T: 28x
  local run_for_minutes=$1
  start_repeating_olap_runner 8 15 5 $initial_queries "ra_8"
  rana_pid=$runner_pid
  start_repeating_olap_runner 8 15 5 $heavier_queries "ra_8_heavy" 8
  rana_heavy_pid=$runner_pid
  start_txn_runner 28  # Implicit: --dataset-type
  txn_pid=$runner_pid

  sleep $(($run_for_minutes * 60))
  kill -INT $rana_pid
  kill -INT $rana_heavy_pid
  kill -INT $txn_pid
  wait $rana_pid
  wait $rana_heavy_pid
  wait $txn_pid
}

function point_five() {
  # A: 28x
  # T: 28x
  local run_for_minutes=$1
  start_repeating_olap_runner 8 2 1 $initial_queries "ra_8"
  rana_pid=$runner_pid
  start_repeating_olap_runner 32 2 1 $heavier_queries "ra_24_heavy" 8
  rana_heavy_pid=$runner_pid
  start_txn_runner 28  # Implicit: --dataset-type
  txn_pid=$runner_pid

  sleep $(($run_for_minutes * 60))
  kill -INT $rana_pid
  kill -INT $rana_heavy_pid
  kill -INT $txn_pid
  wait $rana_pid
  wait $rana_heavy_pid
  wait $txn_pid
}

echo "READY -- Running for 1 hour. Hit Ctrl-C to stop."
# point_one 60
# point_two 60
# point_three 60
# point_four 60
point_five 60

inner_cancel_experiment
