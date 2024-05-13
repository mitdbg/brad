#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh
extract_named_arguments $@

# Resolve paths into absolute paths
abs_txn_config_file=$(realpath $txn_config_file)
abs_physical_config_file=$(realpath $physical_config_file)

sleep 30

run_tpcc_aurora_serverless "t_4"
start_repeating_olap_runner_redshift_serverless 1 10 5 $ra_query_indexes "ch_1"
ra_pid=$runner_pid

sleep $run_for_s

# Shut down.
kill $tpcc_pid
kill $ra_pid
wait $tpcc_pid
wait $ra_pid
