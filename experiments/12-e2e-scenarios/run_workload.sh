#! /bin/bash

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

  if [[ $phys_arg =~ --num-front-ends=.+ ]]; then
    num_front_ends=${phys_arg:17}
  fi

  if [[ $phys_arg =~ --run-for-s=.+ ]]; then
    run_for_s=${phys_arg:12}
  fi
done

function start_brad() {
  pushd ../../
  brad daemon \
    --config-file config/config_cond.yml \
    --schema-name imdb_extended \
    --planner-config-file config/planner.yml \
    --temp-config-file config/temp_config.yml \
    &
  brad_pid=$!
  popd
}

function cancel_experiment() {
  kill -INT $brad_pid
  kill -INT $txn_pid
  kill -INT $ana_pid
}

trap "cancel_experiment" INT
trap "cancel_experiment" TERM

start_brad
sleep 60

# Warm up.
python3 ana_runner.py \
  --num-clients $a_clients \
  --avg-gap-s $a_gap_s \
  --num-front-ends $num_front_ends \
  --query-indexes $query_indexes \
  --run-warmup

python3 ../../workloads/IMDB_extended/run_transactions.py \
  --num-clients $t_clients \
  --num-front-ends $num_front_ends \
  &
txn_pid=$!

python3 ana_runner.py \
  --num-clients $a_clients \
  --avg-gap-s $a_gap_s \
  --num-front-ends $num_front_ends \
  --query-indexes $query_indexes \
  &
ana_pid=$!

sleep $run_for_s

# Invoke the planner and wait for it to complete.
brad cli --command "BRAD_RUN_PLANNER;"

# Send SIGINT to the runner processes.
kill -INT $txn_pid
kill -INT $ana_pid

wait $txn_pid
wait $ana_pid

# Stop BRAD.
kill -INT $brad_pid
