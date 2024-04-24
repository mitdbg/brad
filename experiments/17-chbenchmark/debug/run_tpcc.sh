#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh
extract_named_arguments $@

# Resolve paths into absolute paths
abs_txn_config_file=$(realpath $txn_config_file)
abs_system_config_file=$(realpath $system_config_file)
abs_physical_config_file=$(realpath $physical_config_file)

export BRAD_IGNORE_BLUEPRINT=1
start_brad $abs_system_config_file $abs_physical_config_file

# Wait for BRAD to start up.
sleep 30

# Start the TPC-C workload.
run_tpcc

sleep $run_for_s
sleep 10

kill $tpcc_pid
kill $brad_pid
wait $brad_pid
