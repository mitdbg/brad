#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

initial_queries="99,56,32,92,91,49,30,83,94,38,87,86,76,37,31,46"
heavier_queries="58,61,62,64,69,73,74,51,57,60"

echo "Received args:"
echo $@
echo "---------"

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

function inner_cancel_experiment() {
  if [ ! -z $heavy_rana_pid ]; then
    cancel_experiment $rana_pid $txn_pid $sweep_rana_pid
  else
    cancel_experiment $rana_pid $txn_pid
  fi
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Start with Aurora 2x db.t4g.medium, Redshift off, Athena unused.

# Start with 4 analytical clients.
start_repeating_olap_runner 4 15 5 $initial_queries "ra_4"
rana_pid=$runner_pid
sleep 2

# Start with 4 transactional clients; hold for 10 minutes to stabilize.
start_txn_runner 4 4
txn_pid=$runner_pid

# Keep running for 24 hours (practically "indefinitely").
echo "Runners started. Waiting for 24 hours..."
sleep $((24 * 60 * 60))

# Graceful shutdown.
kill -INT $txn_pid
kill -INT $rana_pid
wait $txn_pid
wait $rana_pid
