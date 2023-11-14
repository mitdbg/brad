#! /bin/bash
script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

initial_queries="99,56,32,92,91,49,30,83,94,38,87,86,76,37,31,46"
heavier_queries="58,61,62,64,69,73,74,51,57,60"

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

# Used just to warm up the systems.

export BRAD_IGNORE_BLUEPRINT=1
start_brad_debug $config_file $planner_config_file
sleep 30

start_repeating_olap_runner 1 5 5 $initial_queries "ra_8"
rana_pid=$runner_pid
sleep 2

log_workload_point "start_txn_2"
start_txn_runner 2
txn_pid=$runner_pid

sleep $((5 * 60))
graceful_shutdown $rana_pid $txn_pid
