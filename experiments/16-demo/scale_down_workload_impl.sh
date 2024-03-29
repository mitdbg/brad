#! /bin/bash

if [ -z $1 ]; then
  >&2 echo "Usage: $0 path/to/physical/config.yml"
  exit 1
fi

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source common.sh

extract_named_arguments $@
start_brad_w_ui $system_config_file $physical_config_file

sleep 30

num_clients=12
starting_clients=4
COND_OUT=out python3 ../../workloads/IMDB_extended/run_transactions_variable_clients.py \
  --num-clients $num_clients \
  --starting-clients $starting_clients \
  --num-front-ends $num_clients
txn_pid=$!

function inner_cancel_experiment() {
  cancel_experiment $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

wait $txn_pid
wait $brad_pid
