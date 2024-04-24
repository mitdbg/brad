#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh
extract_named_arguments $@

# Resolve paths into absolute paths
abs_txn_config_file=$(realpath $txn_config_file)

cd ../../../workloads/chbenchmark/py-tpcc/
RECORD_DETAILED_STATS=1 python3 -m pytpcc.tpcc aurora \
  --no-load \
  --config $abs_txn_config_file \
  --warehouses $txn_warehouses \
  --duration $run_for_s \
  --clients $t_clients \
  --scalefactor $txn_scale_factor
