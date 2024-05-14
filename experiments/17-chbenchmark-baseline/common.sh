function run_tpcc_tidb() {
  local results_name=$1
  local args=(
    --no-load
    --warehouses $txn_warehouses
    --duration $run_for_s
    --clients $t_clients
    --scalefactor $txn_scale_factor
  )
  if [[ ! -z $txn_zipfian_alpha ]]; then
    args+=(--zipfian-alpha $txn_zipfian_alpha)
  fi
  results_dir=$EXPT_OUT/$results_name
  mkdir -p $results_dir
  RECORD_DETAILED_STATS=1 COND_OUT=$results_dir python3 -m pytpcc.tpcc tidb "${args[@]}" &
  tpcc_pid=$!
}

function log_workload_point() {
  msg=$1
  now=$(date "+%Y-%m-%d %H:%M:%S")
  msg="$now,$msg"
  echo "$msg" >> $EXPT_OUT/points.log
  echo "$msg"
}

function start_repeating_olap_runner() {
  local ra_clients=$1
  local ra_gap_s=$2
  local ra_gap_std_s=$3
  local query_indexes=$4
  local results_name=$5
  local client_offset=$6

  local args=(
    --num-clients $ra_clients
    --num-front-ends $num_front_ends
    --query-indexes $query_indexes
    --query-bank-file $ra_query_bank_file
    --avg-gap-s $ra_gap_s
    --avg-gap-std-s $ra_gap_std_s
  )

  if [[ ! -z $ra_query_frequency_path ]]; then
    args+=(--query-frequency-path $ra_query_frequency_path)
  fi

  if [[ ! -z $client_offset ]]; then
    args+=(--client-offset $client_offset)
  fi

  >&2 echo "[Serial Repeating Analytics] Running with $ra_clients..."
  results_dir=$COND_OUT/$results_name
  mkdir -p $results_dir

  log_workload_point $results_name
  COND_OUT=$results_dir python3.11 ../../../workloads/IMDB_extended/run_repeating_analytics_serial.py "${args[@]}" &

  # This is a special return value variable that we use.
  runner_pid=$!
}

function start_repeating_olap_runner_tidb() {
  local ra_clients=$1
  local ra_gap_s=$2
  local ra_gap_std_s=$3
  local query_indexes=$4
  local results_name=$5
  
  engine=$ANALYTICS_ENGINE
  bank_file=$ra_query_bank_file
  results_dir=$EXPT_OUT/$results_name
  mkdir -p $results_dir

  local args=(
    --num-clients $ra_clients
    --query-indexes "$query_indexes"
    --query-bank-file $bank_file
    --avg-gap-s $ra_gap_s
    --avg-gap-std-s $ra_gap_std_s
    --baseline $engine
    --output-dir $results_dir
  )

  >&2 echo "[Serial Repeating Analytics] Running with $ra_clients..."

  log_workload_point $results_name
  python3 workloads/IMDB_extended/run_repeating_analytics_serial.py "${args[@]}" &

  # This is a special return value variable that we use.
  runner_pid=$!
}

function graceful_shutdown() {
  for pid_var in "$@"; do
    kill -INT $pid_var
  done
  for pid_var in "$@"; do
    wait $pid_var
  done

  kill -INT $brad_pid
  wait $brad_pid
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

    if [[ $phys_arg =~ --t-clients=.+ ]]; then
      t_clients=${phys_arg:12}
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

    if [[ $phys_arg =~ --physical-config-file=.+ ]]; then
      physical_config_file=${phys_arg:23}
    fi

    if [[ $phys_arg =~ --system-config-file=.+ ]]; then
      system_config_file=${phys_arg:21}
    fi

    if [[ $phys_arg =~ --schema-name=.+ ]]; then
      schema_name=${phys_arg:14}
    fi

    if [[ $phys_arg =~ --query-sequence-file=.+ ]]; then
      query_sequence_file=${phys_arg:22}
    fi

    if [[ $phys_arg =~ --txn-scale-factor=.+ ]]; then
      txn_scale_factor=${phys_arg:19}
    fi

    if [[ $phys_arg =~ --txn-warehouses=.+ ]]; then
      txn_warehouses=${phys_arg:17}
    fi

    if [[ $phys_arg =~ --txn-config-file=.+ ]]; then
      txn_config_file=${phys_arg:18}
    fi

    if [[ $phys_arg =~ --txn-zipfian-alpha=.+ ]]; then
      txn_zipfian_alpha=${phys_arg:20}
    fi
  done
}

