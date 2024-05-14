#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

start_brad $config_file $planner_config_file
sleep 30

# 1 client, repeat query list 5 times
run_repeating_olap_warmup 1 5

>&2 echo "Experiment done. Shutting down."

kill -INT $brad_pid
wait $brad_pid
