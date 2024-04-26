#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source common.sh

extract_named_arguments $@
start_brad_w_ui $system_config_file $physical_config_file

>&2 echo "Waiting for BRAD to start up..."
sleep 30

num_clients=24
starting_clients=4
COND_OUT=out python3 ../../workloads/IMDB_extended/run_transactions_variable_clients.py \
  --num-clients $num_clients \
  --starting-clients $starting_clients \
  --num-front-ends $num_clients \
  &
txn_pid=$!
>&2 echo "Started T runner (PID $txn_pid)"

qidx="99,56,32,92,91,49,30,83,94,87,86,76,37,31,46"
qbank_file="../../workloads/IMDB_100GB/regular_test/queries.sql"
ra_gap_s=15
ra_gap_std_s=5
starting_ra_clients=8
COND_OUT=out python3 ../../workloads/IMDB_extended/run_variable_clients.py \
  --num-clients $num_clients \
  --starting-clients $starting_ra_clients \
  --num-front-ends $num_clients \
  --query-indexes $qidx \
  --query-bank-file $qbank_file \
  --avg-gap-s $ra_gap_s \
  --avg-gap-std-s $ra_gap_std_s \
  &
rana_pid=$!
>&2 echo "Started RA runner (PID $rana_pid)"

# Wait for Ctrl-C.
read -r -d '' _ </dev/tty

graceful_shutdown $txn_pid $rana_pid
