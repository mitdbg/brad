function cancel_experiment() {
  for pid_var in "$@"; do
    kill -INT $pid_var
  done
}

function graceful_shutdown() {
  for pid_var in "$@"; do
    kill -INT $pid_var
  done
  for pid_var in "$@"; do
    wait $pid_var
  done
}

function log_workload_point() {
  msg=$1
  now=$(date "+%Y-%m-%d %H:%M:%S")
  msg="$now,$msg"
  echo "$msg" >> $EXPT_OUT/points.log
  echo "$msg"
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
  local query_indexes=$4
  local results_name=$5

  results_dir=$EXPT_OUT/$results_name
  mkdir -p $results_dir

  # If result name contains "vec", use $VECTOR_ENGINE, otherwise use $ANALYTICS_ENGINE
  if [[ $results_name == *"vec"* ]]; then
    engine=$VECTOR_ENGINE
    bank_file=$vector_query_bank_file
  else
    engine=$ANALYTICS_ENGINE
    bank_file=$ra_query_bank_file
  fi


  local args=(
    --num-clients $ra_clients
    --query-indexes "$query_indexes"
    --query-bank-file $bank_file
    --avg-gap-s $ra_gap_s
    --avg-gap-std-s $ra_gap_std_s
    --baseline $engine
    --output-dir $results_dir
  )

  if [[ ! -z $ra_query_frequency_path ]]; then
    args+=(--query-frequency-path $ra_query_frequency_path)
  fi

  >&2 echo "[Repeating Analytics] Running with $ra_clients..."

  log_workload_point $results_name
  python3 workloads/IMDB_extended/run_repeating_analytics.py "${args[@]}" &

  # This is a special return value variable that we use.
  runner_pid=$!
}

function start_seq_olap_runner() {
  local num_clients=$1
  local gap_s=$2
  local gap_std_s=$3
  local results_name=$4

  results_dir=$EXPT_OUT/$results_name
  mkdir -p $results_dir

  local args=(
    --num-clients $num_clients
    --query-sequence-file $seq_query_bank_file
    --avg-gap-s $gap_s
    --avg-gap-std-s $gap_std_s
    --baseline $ANALYTICS_ENGINE
    --output-dir $results_dir
  )

  >&2 echo "[Seq Analytics] Running with $num_clients..."

  log_workload_point $results_name
  python3 workloads/IMDB_extended/run_query_sequence.py "${args[@]}" &

  # This is a special return value variable that we use.
  runner_pid=$!
}

function start_txn_runner() {
  t_clients=$1

  >&2 echo "[Transactions] Running with $t_clients..."
  results_dir=$EXPT_OUT/t_${t_clients}
  mkdir -p $results_dir

  log_workload_point "txn_${t_clients}"
  python3 workloads/IMDB_extended/run_transactions.py \
    --num-clients $t_clients \
    --output-dir $results_dir \
    --baseline $TRANSACTION_ENGINE \
    &

  # This is a special return value variable that we use.
  runner_pid=$!
}

function start_txn_runner_serial() {
  t_clients=$1

  >&2 echo "[Serial Transactions] Running with $t_clients..."
  results_dir=$EXPT_OUT/t_${t_clients}
  mkdir -p $results_dir

  local args=(
    --num-clients $t_clients
    --output-dir $results_dir
    --baseline $TRANSACTION_ENGINE
  )

  log_workload_point "txn_${t_clients}"
  python3 workloads/IMDB_extended/run_transactions_serial.py \
    "${args[@]}" &

  # This is a special return value variable that we use.
  runner_pid=$!
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

    if [[ $phys_arg =~ --ra-query-frequency-path=.+ ]]; then
      ra_query_frequency_path=${phys_arg:26}
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