#! /bin/bash



ANALYTICS_ENGINE="tidb"
TRANSACTION_ENGINE="tidb"
EXPT_OUT="expt_out_chbench_${ANALYTICS_ENGINE}_scale_down"
script_loc=$(cd $(dirname $0) && pwd -P)
source $script_loc/../common.sh

# Params
# "txn-warehouses": 1740,
# "txn-scale-factor": 1,  # TBD
# "t-clients": 4,  # TBD
# "num-front-ends": 5, # TBD
# "run-for-s": 2 * 60 * 60,  # 2 hours
# "txn-zipfian-alpha": ZIPFIAN_ALPHA,
txn_warehouses=1740
txn_scale_factor=1
t_clients=4
num_front_ends=5
txn_zipfian_alpha=5
run_for_s=$(( 2 * 60 * 60 ))



# Extract args.
extract_named_arguments $@

# Just fo testing.
echo "Query indexes"
echo $ra_query_indexes
echo "rana Query Bank"
# Check file and exit if not exists
ls $ra_query_bank_file || exit 1

run_tpcc_tidb "t_4"
start_repeating_olap_runner_tidb 1 10 5 $ra_query_indexes "ch_1"
ra_pid=$runner_pid

sleep $run_for_s

# Shut down.
kill $tpcc_pid
kill $ra_pid
wait $tpcc_pid
wait $ra_pid
