#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

extract_named_arguments $@

query_indices="62,64,65,66,69,72,73,74,91,59"

export BRAD_IGNORE_BLUEPRINT=1
start_brad_debug $system_config_file $physical_config_file
sleep 30

start_other_repeating_runner 1 5 5 "ra_vector" 1
other_pid=$runner_pid
sleep 30

start_repeating_olap_runner 1 5 5 $query_indices "ra_1"
rana_pid=$runner_pid

start_txn_runner 4  # Implicit: --dataset-type
txn_pid=$runner_pid

sleep $((5 * 60))
>&2 echo "Warm up done. Shutting down..."
graceful_shutdown $rana_pid $txn_pid $other_pid
