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

  if [[ $phys_arg =~ --t-clients-lo=.+ ]]; then
    t_clients_lo=${phys_arg:15}
  fi

  if [[ $phys_arg =~ --t-clients-hi=.+ ]]; then
    t_clients_hi=${phys_arg:15}
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

# Start the analytical runner.
log_workload_point "ana_${a_clients}"
python3 ana_runner.py \
  --num-clients $a_clients \
  --avg-gap-s $a_gap_s \
  --avg-gap-std-s $a_gap_std_s \
  --num-front-ends $num_front_ends \
  --query-indexes $query_indexes \
  &
ana_pid=$!

function run_t_workload() {
  t_clients=$1

  >&2 echo "Running with $t_clients..."
  results_dir=$COND_OUT/t_${t_clients}
  mkdir $results_dir

  log_workload_point "txn_${t_clients}"
  COND_OUT=$results_dir python3 ../../workloads/IMDB_extended/run_transactions.py \
    --num-clients $t_clients \
    --num-front-ends $num_front_ends \
    &
  txn_pid=$!

  sleep $run_for_s
  >&2 echo "Done with $t_clients"

  if [ $t_clients != $t_clients_hi ]; then
    kill -INT $txn_pid
    wait $txn_pid
  else
    echo >&2 "Leaving $t_clients running to invoke the planner."
  fi
}

if [ $t_clients_lo = 1 ]; then
  run_t_workload 1
fi

# Run with an increasing number of transactional clients.
# Start from 2 clients and always add 2 (to keep an even number of clients).
for t_clients in $(seq 2 2 $t_clients_hi); do
  run_t_workload $t_clients
done

# Need to sleep an extra 3 minutes before starting the planner to ensure the
# metrics catch up.
sleep 180

# Run the planner and wait for it to complete.
log_workload_point "invoke_planner"
brad cli --command "BRAD_RUN_PLANNER;"

# Shut down everything now.
>&2 echo "Scenario done. Shutting down runners..."

kill -INT $txn_pid
kill -INT $ana_pid

wait $txn_pid
wait $ana_pid

>&2 echo "Shutting down BRAD..."
kill -INT $brad_pid
wait $brad_pid
