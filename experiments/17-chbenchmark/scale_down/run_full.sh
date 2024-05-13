#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh
extract_named_arguments $@

# Resolve paths into absolute paths
abs_txn_config_file=$(realpath $txn_config_file)
abs_system_config_file=$(realpath $system_config_file)
abs_physical_config_file=$(realpath $physical_config_file)

start_brad $abs_system_config_file $abs_physical_config_file

sleep 30

run_tpcc "t_4"
start_repeating_olap_runner 1 10 5 $ra_query_indexes "ch_1" $t_clients
ra_pid=$runner_pid

sleep $run_for_s

# Shut down.
graceful_shutdown $tpcc_pid $ra_pid
