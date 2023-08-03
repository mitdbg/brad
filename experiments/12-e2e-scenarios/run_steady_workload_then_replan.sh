#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source common.sh

# Evaluates any environment variables in this script's arguments. This script
# should only be run on trusted input.
orig_args=($@)
for val in "${orig_args[@]}"; do
  phys_arg=$(eval "echo $val")

  if [[ $phys_arg =~ --a-clients=.+ ]]; then
    a_clients=${phys_arg:12}
  fi

  if [[ $phys_arg =~ --t-clients=.+ ]]; then
    t_clients=${phys_arg:12}
  fi

  if [[ $phys_arg =~ --query-indexes=.+ ]]; then
    query_indexes=${phys_arg:16}
  fi

  if [[ $phys_arg =~ --a-gap-s=.+ ]]; then
    a_gap_s=${phys_arg:10}
  fi

  if [[ $phys_arg =~ --a-gap-std-s=.+ ]]; then
    a_gap_std_s=${phys_arg:14}
  fi

  if [[ $phys_arg =~ --num-front-ends=.+ ]]; then
    num_front_ends=${phys_arg:17}
  fi

  if [[ $phys_arg =~ --run-for-s=.+ ]]; then
    run_for_s=${phys_arg:12}
  fi

  if [[ $phys_arg =~ --config-file=.+ ]]; then
    config_file=${phys_arg:14}
  fi
done

trap "cancel_experiment" INT
trap "cancel_experiment" TERM

start_brad $config_file
sleep 30

# Warm up.
log_workload_point "warmup"
python3 ana_runner.py \
  --num-clients $a_clients \
  --avg-gap-s $a_gap_s \
  --avg-gap-std-s $a_gap_std_s \
  --num-front-ends $num_front_ends \
  --query-indexes $query_indexes \
  --run-warmup

log_workload_point "txn_${t_clients}"
python3 ../../workloads/IMDB_extended/run_transactions.py \
  --num-clients $t_clients \
  --num-front-ends $num_front_ends \
  &
txn_pid=$!

log_workload_point "ana_${a_clients}"
python3 ana_runner.py \
  --num-clients $a_clients \
  --avg-gap-s $a_gap_s \
  --avg-gap-std-s $a_gap_std_s \
  --num-front-ends $num_front_ends \
  --query-indexes $query_indexes \
  &
ana_pid=$!

sleep $run_for_s

# Invoke the planner and wait for it to complete.
log_workload_point "invoke_planner"
brad cli --command "BRAD_RUN_PLANNER;"

# Send SIGINT to the runner processes.
kill -INT $txn_pid
kill -INT $ana_pid

wait $txn_pid
wait $ana_pid

# Stop BRAD.
kill -INT $brad_pid
wait $brad_pid
