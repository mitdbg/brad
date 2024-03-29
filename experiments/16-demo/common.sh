function start_brad_w_ui() {
  system_config_file=$1
  physical_config_file=$2
  curr_dir=$(pwd)

  pushd ../../
  brad daemon \
    --physical-config-file $physical_config_file \
    --system-config-file $curr_dir/$system_config_file \
    --schema-name $schema_name \
    --ui \
    &
  brad_pid=$!
  popd
}

function cancel_experiment() {
  for pid_var in "$@"; do
    kill -INT $pid_var
  done
  kill -INT $brad_pid
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

function terminate_process_group() {
  local pid=$1
  local initial_wait_s=$2
  sleep $2
  if kill -0 $pid >/dev/null 2>&1; then
    pkill -KILL -P $pid
    pkill -KILL $pid
    echo "NOTE: Forced process $pid to stop."
  else
    echo "Process $pid stopped gracefully."
  fi
}

function log_workload_point() {
  msg=$1
  now=$(date --utc "+%Y-%m-%d %H:%M:%S")
  echo "$now,$msg" >> $COND_OUT/points.log
}

function pause_for_s_past_timepoint() {
  local timepoint="$1"
  local wait_s="$2"

  local curr_tp="$(date -u +%s)"
  elapsed_s="$(($curr_tp - $timepoint))"
  if (( $elapsed_s < $wait_s )); then
    leftover_s=$(($wait_s - $elapsed_s))
    >&2 echo "Waiting $leftover_s seconds before continuing..."
    sleep $leftover_s
  fi
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

    if [[ $phys_arg =~ --other-query-bank-file=.+ ]]; then
      other_query_bank_file=${phys_arg:24}
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

    if [[ $phys_arg =~ --skip-replan=.+ ]]; then
      skip_replan=${phys_arg:14}
    fi

    if [[ $phys_arg =~ --schema-name=.+ ]]; then
      schema_name=${phys_arg:14}
    fi

    if [[ $phys_arg =~ --dataset-type=.+ ]]; then
      dataset_type=${phys_arg:15}
    fi

    if [[ $phys_arg =~ --query-sequence-file=.+ ]]; then
      query_sequence_file=${phys_arg:22}
    fi

    if [[ $phys_arg =~ --snowset-query-frequency-path=.+ ]]; then
      snowset_query_frequency_path=${phys_arg:31}
    fi

    if [[ $phys_arg =~ --snowset-client-dist-path=.+ ]]; then
      snowset_client_dist_path=${phys_arg:27}
    fi

    if [[ $phys_arg =~ --snowset-gap-dist-path=.+ ]]; then
      snowset_gap_dist_path=${phys_arg:24}
    fi

    if [[ $phys_arg =~ --txn-scale-factor=.+ ]]; then
      txn_scale_factor=${phys_arg:19}
    fi

    if [[ $phys_arg =~ --is-daylong-hd=.+ ]]; then
      is_daylong_hd=1
    fi
  done
}
