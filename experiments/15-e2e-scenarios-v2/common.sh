function start_brad() {
  config_file=$1

  pushd ../../../
  brad daemon \
    --config-file $config_file \
    --schema-name $schema_name \
    --planner-config-file $planner_config_file \
    --temp-config-file config/temp_config_sample.yml \
    &
  brad_pid=$!
  popd
}

function cancel_experiment() {
  kill -INT $txn_pid
  kill -INT $rana_pid
  kill -INT $brad_pid
}

function graceful_shutdown() {
  kill -INT $txn_pid
  kill -INT $rana_pid
  wait $txn_pid
  wait $rana_pid

  kill -INT $brad_pid
  wait $brad_pid
}

function log_workload_point() {
  msg=$1
  now=$(date --utc "+%Y-%m-%d %H:%M:%S")
  echo "$now,$msg" >> $COND_OUT/points.log
}

function poll_file_for_event() {
  local file="$1"
  local event_name="$2"
  local timeout_minutes="$3"
  local previous_size=$(stat -c %s "$file")
  local current_size
  local last_line

  local start_time
  local elapsed_time
  start_time=$(date +%s)

  while true; do
    current_size=$(stat -c %s "$file")

    if [[ $current_size -ne $previous_size ]]; then
      last_line=$(tail -n 1 "$file")

      if [[ $last_line == *"$event_name"* ]]; then
        >&2 echo "Detected new $event_name!"
        break
      fi
    fi

    elapsed_time=$(( $(date +%s) - $start_time ))
    if [[ $elapsed_time -ge $((timeout_minutes * 60)) ]]; then
      >&2 echo "Timeout reached. Did not detect $event_name within $timeout_minutes minutes."
      log_workload_point "timeout_poll_${event_name}"
      break
    fi

    sleep 30
  done
}

function start_repeating_olap_runner() {
  local ra_clients=$1
  local ra_gap_s=$2
  local ra_gap_std_s=$3

  >&2 echo "[Repeating Analytics] Running with $ra_clients..."
  results_dir=$COND_OUT/ra_${ra_clients}
  mkdir $results_dir

  log_workload_point "rana_${ra_clients}"
  COND_OUT=$results_dir python3 ../../../workloads/IMDB_extended/run_repeating_analytics.py \
    --num-clients $ra_clients \
    --avg-gap-s $ra_gap_s \
    --avg-gap-std-s $ra_gap_std_s \
    --num-front-ends $num_front_ends \
    --query-indexes $ra_query_indexes \
    --query-bank-file $ra_query_bank_file \
    &
  rana_pid=$!
}

function start_txn_runner() {
  t_clients=$1

  >&2 echo "[Transactions] Running with $t_clients..."
  results_dir=$COND_OUT/t_${t_clients}
  mkdir $results_dir

  log_workload_point "txn_${t_clients}"
  COND_OUT=$results_dir python3 ../../../workloads/IMDB_extended/run_transactions.py \
    --num-clients $t_clients \
    --num-front-ends $num_front_ends \
    &
  txn_pid=$!
}

function extract_named_arguments() {
  # Evaluates any environment variables in this script's arguments. This script
  # should only be run on trusted input.
  orig_args=($@)
  for val in "${orig_args[@]}"; do
    phys_arg=$(eval "echo $val")

    if [[ $phys_arg =~ --ra-clients=.+ ]]; then
      ra_clients=${phys_arg:13}
    fi

    if [[ $phys_arg =~ --t-clients-lo=.+ ]]; then
      t_clients_lo=${phys_arg:15}
    fi

    if [[ $phys_arg =~ --t-clients-hi=.+ ]]; then
      t_clients_hi=${phys_arg:15}
    fi

    if [[ $phys_arg =~ --ra-query-indexes=.+ ]]; then
      ra_query_indexes=${phys_arg:19}
    fi

    if [[ $phys_arg =~ --ra-query-bank-file=.+ ]]; then
      ra_query_bank_file=${phys_arg:21}
    fi

    if [[ $phys_arg =~ --ra-gap-s=.+ ]]; then
      ra_gap_s=${phys_arg:11}
    fi

    if [[ $phys_arg =~ --ra-gap-std-s=.+ ]]; then
      ra_gap_std_s=${phys_arg:15}
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

    if [[ $phys_arg =~ --planner-config-file=.+ ]]; then
      planner_config_file=${phys_arg:22}
    fi

    if [[ $phys_arg =~ --skip-replan=.+ ]]; then
      skip_replan=${phys_arg:14}
    fi

    if [[ $phys_arg =~ --schema-name=.+ ]]; then
      schema_name=${phys_arg:14}
    fi
  done
}
