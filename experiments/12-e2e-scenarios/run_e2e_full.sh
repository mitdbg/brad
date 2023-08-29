#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source common.sh

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=($@)
for val in "${orig_args[@]}"; do
  phys_arg=$(eval "echo $val")

  if [[ $phys_arg =~ --query-indexes=.+ ]]; then
    query_indexes=${phys_arg:16}
  fi

  if [[ $phys_arg =~ --num-front-ends=.+ ]]; then
    num_front_ends=${phys_arg:17}
  fi

  if [[ $phys_arg =~ --config-file=.+ ]]; then
    config_file=${phys_arg:14}
  fi

  if [[ $phys_arg =~ --planner-config-file=.+ ]]; then
    planner_config_file=${phys_arg:22}
  fi

  if [[ $phys_arg =~ --skip-replan=.+ ]]; then
    skip_replan=${phys_arg:14}
  fi
done

trap "cancel_experiment" INT
trap "cancel_experiment" TERM

start_auto_brad $config_file $planner_config_file
sleep 30

# 1x A, 1x T
start_ana_runner 1 30 5
start_txn_runner 1

# Wait until a re-plan and transition completes (15 minute timeout).
poll_file_for_event $COND_OUT/brad_daemon_events.csv "post_transition_completed" 15
log_workload_point "after_scale_down_replan"

# Wait 3 more minutes before proceeding.
sleep 180

log_workload_point "increasing_txn_clients_start"

# Wait for existing clients to complete.
kill -INT $txn_pid
wait $txn_pid

# Increase scale up to 10 clients.
for num_t_clients in $(seq 2 2 10); do
  >&2 echo "Running with $num_t_clients T clients..."
  log_workload_point "txn_${num_t_clients}"
  start_txn_runner $num_t_clients
  sleep 300  # Wait 5 minutes

  if [ $num_t_clients != 10 ]; then
    kill -INT $txn_pid
    wait $txn_pid
  else
    >&2 echo "Scaled up to 10 T clients."
  fi
done

# Change the analytics workload frequency.
log_workload_point "increasing_ana_clients_start"
kill -INT $ana_pid
wait $ana_pid

>&2 echo "Scaled up to 3 A clients."
start_ana_runner 3 3 1

# Wait until a re-plan and transition completes (20 minute timeout).
poll_file_for_event $COND_OUT/brad_daemon_events.csv "post_transition_completed" 20
log_workload_point "after_analytics_replan"

sleep 600  # Wait 10 more minutes.
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."

kill -INT $txn_pid
kill -INT $ana_pid
wait $txn_pid
wait $ana_pid

>&2 echo "Shutting down BRAD..."
kill -INT $brad_pid
wait $brad_pid
