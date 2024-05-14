#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
EXPT_OUT="expt_out_ana_up"
mkdir -p $EXPT_OUT
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
    start_txn_runner $t_clients  # Implicit: --dataset-type
    txn_pid=$runner_pid

    sleep $(($gap_minute * 60))
    if [[ -z $keep_last ]] || [[ $t_clients != $keep_last ]]; then
      kill -INT $txn_pid
      wait $txn_pid
    fi
  done
}

function rana_sweep_offset4() {
  local sweep=$1
  local gap_minute=$2
  local keep_last=$3
  local query_indices=$4
  local gap_time_s=$5

  for ra_clients in $sweep; do
    start_repeating_olap_runner $ra_clients $gap_time_s 1 $query_indices "ra_sweep_${ra_clients}" 4
    sweep_rana_pid=$runner_pid

    sleep $(($gap_minute * 60))
    if [[ -z $keep_last ]] || [[ $ra_clients != $keep_last ]]; then
      kill -INT $sweep_rana_pid
      wait $sweep_rana_pid
    fi
  done
}

function inner_cancel_experiment() {
  if [ ! -z $sweep_rana_pid ]; then
    cancel_experiment $rana_pid $txn_pid $sweep_rana_pid
  else
    cancel_experiment $rana_pid $txn_pid
  fi
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Start with 4 light analytical clients.
log_workload_point "start_rana_4"
start_repeating_olap_runner 4 15 5 $initial_queries "ra_4"
rana_pid=$runner_pid
log_workload_point "started_rana_4_$rana_pid"
sleep 2

# Start with 4 transactional clients; hold for 10 minutes to stabilize.
log_workload_point "start_txn_4"
start_txn_runner 4
txn_pid=$runner_pid
sleep $((10 * 60)) # 10 mins cumulative

# Scale up to 60 analytical clients in total (56 heavy).
log_workload_point "start_increase_rana_heavy_4_to_56"
rana_sweep_offset4 "4 8 12 16 20 24 32 40 48 56" 5 56 $heavier_queries 3 1
log_workload_point "hold_rana_heavy_56"
sleep $((120 * 60))  # 50 + 120 mins; 180 mins cumulative

log_workload_point "experiment_workload_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $sweep_rana_pid $txn_pid
log_workload_point "shutdown_complete"