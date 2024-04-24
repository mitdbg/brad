function start_brad() {
  system_config_file=$1
  physical_config_file=$2

  pushd ../../../
  brad daemon \
    --physical-config-file $physical_config_file \
    --system-config-file $system_config_file \
    --schema-name $schema_name \
    &
  brad_pid=$!
  popd
}

function run_tpcc() {
  pushd ../../../workloads/chbenchmark/py-tpcc/
  RECORD_DETAILED_STATS=1 python3 -m pytpcc.tpcc brad \
    --no-load \
    --config $abs_txn_config_file \
    --warehouses $txn_warehouses \
    --duration $run_for_s \
    --clients $t_clients \
    --scalefactor $txn_scale_factor &
  tpcc_pid=$!
  popd
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
  done
}

