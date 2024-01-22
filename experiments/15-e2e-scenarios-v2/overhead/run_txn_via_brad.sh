#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --config-file
# --planner-config-file
extract_named_arguments $@

start_brad $config_file $planner_config_file
sleep 30

start_txn_runner 2  # Implicit: --dataset-type
>&2 echo "Waiting for 3 minutes..."
sleep 180  # 3 minutes

>&2 echo "Experiment done. Shutting down."

kill -INT $txn_pid
wait $txn_pid
kill -INT $brad_pid
wait $brad_pid
