#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
EXPT_OUT="expt_out"
ANALYTICS_ENGINE="redshift"
TRANSACTION_ENGINE="aurora"
initial_queries="99,56,32,92,91,49,30,83,94,38,87,86,76,37,31,46"
heavier_queries="58,61,62,64,69,73,74,51,57,60"
source $script_loc/../common.sh


# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

function txn_sweep() {
  local sweep=$1
  local gap_minute=$2
  local keep_last=$3

  for t_clients in $sweep; do
    echo "Sweeping. Started txn runner with $t_clients clients"
    start_txn_runner $t_clients  # Implicit: --dataset-type
    txn_pid=$runner_pid

    sleep $(($gap_minute * 60))
    if [[ -z $keep_last ]] || [[ $t_clients != $keep_last ]]; then
      kill -INT $txn_pid
      wait $txn_pid
    fi
  done
}

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Start with 4 analytical clients.
log_workload_point "start_rana_4"
start_repeating_olap_runner 4 15 5 $initial_queries "ra_4"
rana_pid=$runner_pid
log_workload_point "started_rana_4_$rana_pid"
sleep 2

# Start with 4 transactional clients; hold for 15 minutes to stabilize.
log_workload_point "start_txn_4"
start_txn_runner 4
txn_pid=$runner_pid
echo "Started txn runner with 4 clients"
sleep $((10 * 60))  # 10 mins; 10 mins cumulative

# Scale up to 8 transactional clients and hold for 30 minutes.
log_workload_point "start_increase_txn_4_to_8"
kill -INT $txn_pid
wait $txn_pid
txn_sweep "5 6 7 8" 3 8
log_workload_point "hold_txn_8_20_min"
sleep $((35 * 60))  # 47 mins total; 57 mins cumulative

# Shut down everything now.
log_workload_point "experiment_workload_done"
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
