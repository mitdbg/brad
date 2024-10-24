#! /bin/bash

# This is used for testing the workload intensity against different
# engine configurations.

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

initial_queries="99,56,32,92,91,49,30,83,94,38,87,86,76,37,31,46"
heavier_queries="58,61,62,64,69,73,74,51,57,60"

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

function inner_cancel_experiment() {
  if [ ! -z $sweep_rana_pid ]; then
    cancel_experiment $rana_pid $txn_pid $sweep_rana_pid
  else
    cancel_experiment $rana_pid $txn_pid
  fi
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

export BRAD_IGNORE_BLUEPRINT=1
start_brad $system_config_file $physical_config_file
sleep 30

# Start with 4 light analytical clients.
start_repeating_olap_runner 4 15 5 $initial_queries "ra_4"
rana_pid=$runner_pid
sleep 2

# Start with 4 transactional clients; hold for 10 minutes to stabilize.
start_txn_runner 4
txn_pid=$runner_pid
sleep 2

# Scale up to 60 analytical clients in total (56 heavy).
start_repeating_olap_runner 56 3 1 $heavier_queries "ra_heavy_56" 4
sweep_rana_pid=$runner_pid
sleep $((60 * 60))

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $sweep_rana_pid $txn_pid
