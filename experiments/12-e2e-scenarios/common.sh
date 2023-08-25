function start_brad() {
  config_file=$1

  pushd ../../
  BRAD_PERSIST_BLUEPRINT=1 brad daemon \
    --config-file $config_file \
    --schema-name imdb_extended \
    --planner-config-file config/planner.yml \
    --temp-config-file config/temp_config_sample.yml \
    &
  brad_pid=$!
  popd
}

function start_auto_brad() {
  config_file=$1

  pushd ../../
  brad daemon \
    --config-file $config_file \
    --schema-name imdb_extended \
    --planner-config-file $planner_config_file \
    --temp-config-file config/temp_config_sample.yml \
    &
  brad_pid=$!
  popd
}

function cancel_experiment() {
  kill -INT $brad_pid
  kill -INT $txn_pid
  kill -INT $ana_pid
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
      break
    fi

    sleep 30
  done
}

function start_ana_runner() {
  local a_clients=$1
  local a_gap_s=$2
  local a_gap_std_s=$3

  >&2 echo "Running with $a_clients..."
  results_dir=$COND_OUT/a_${a_clients}
  mkdir $results_dir

  log_workload_point "ana_${a_clients}"
  COND_OUT=$results_dir python3 ana_runner.py \
    --num-clients $a_clients \
    --avg-gap-s $a_gap_s \
    --avg-gap-std-s $a_gap_std_s \
    --num-front-ends $num_front_ends \
    --query-indexes $query_indexes \
    &
  ana_pid=$!
}

function start_txn_runner() {
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
}
