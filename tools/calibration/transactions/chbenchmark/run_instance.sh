#! /bin/bash

function extract_named_arguments() {
  # Evaluates any environment variables in this script's arguments. This script
  # should only be run on trusted input.
  orig_args=($@)
  for val in "${orig_args[@]}"; do
    phys_arg=$(eval "echo $val")

    if [[ $phys_arg =~ --t-clients=.+ ]]; then
      t_clients=${phys_arg:12}
    fi

    if [[ $phys_arg =~ --run-for-s=.+ ]]; then
      run_for_s=${phys_arg:12}
    fi

    if [[ $phys_arg =~ --system-config-file=.+ ]]; then
      system_config_file=${phys_arg:21}
    fi

    if [[ $phys_arg =~ --physical-config-file=.+ ]]; then
      physical_config_file=${phys_arg:23}
    fi

    if [[ $phys_arg =~ --txn-warehouses=.+ ]]; then
      txn_warehouses=${phys_arg:17}
    fi

    if [[ $phys_arg =~ --txn-config-file=.+ ]]; then
      txn_config_file=${phys_arg:18}
    fi

    if [[ $phys_arg =~ --schema-name=.+ ]]; then
      schema_name=${phys_arg:14}
    fi

    if [[ $phys_arg =~ --instance=.+ ]]; then
      instance=${phys_arg:11}
    fi
  done
}

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
extract_named_arguments $@

abs_txn_config_file=$(realpath $txn_config_file)
abs_physical_config_file=$(realpath $physical_config_file)
abs_system_config_file=$(realpath $system_config_file)

>&2 echo "Adjusting blueprint"
brad admin --debug modify_blueprint \
  --schema-name $schema_name \
  --physical-config-file $abs_physical_config_file \
  --system-config-file $abs_system_config_file \
  --aurora-instance-type $instance \
  --aurora-num-nodes 1

>&2 echo "Waiting 30 seconds before retrieving pre-metrics..."
sleep 30

>&2 echo "Retrieving pre-metrics..."
python3 retrieve_metrics.py --out-file $COND_OUT/pi_metrics_before.csv --physical-config-file $abs_physical_config_file

>&2 echo "Running the transactional workload..."

# We run against Aurora directly.
pushd ../../../../workloads/chbenchmark/py-tpcc/
RECORD_DETAILED_STATS=1 python3 -m pytpcc.tpcc aurora \
  --no-load \
  --config $abs_txn_config_file \
  --warehouses $txn_warehouses \
  --duration $run_for_s \
  --clients $t_clients \
  --scalefactor 1 \
  --lat-sample-prob 0.25
popd

>&2 echo "Waiting 10 seconds before retrieving metrics..."
sleep 10

>&2 echo "Retrieving metrics..."
python3 retrieve_metrics.py --out-file $COND_OUT/pi_metrics.csv --physical-config-file $abs_physical_config_file
