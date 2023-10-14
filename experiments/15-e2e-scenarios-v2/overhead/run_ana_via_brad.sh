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

start_repeating_olap_runner 1 10 5
>&2 echo "Waiting for 3 minutes..."
sleep 180  # 3 minutes

>&2 echo "Experiment done. Shutting down."

kill -INT $rana_pid
wait $rana_pid
kill -INT $brad_pid
wait $brad_pid
