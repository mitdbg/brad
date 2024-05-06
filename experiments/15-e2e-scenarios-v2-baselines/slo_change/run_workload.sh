#! /bin/bash

EXPT_OUT="expt_out"
ANALYTICS_ENGINE="tidb"
TRANSACTION_ENGINE="tidb"
script_loc=$(cd $(dirname $0) && pwd -P)
total_first_phase_time_s=$(( 10 * 60 ))
total_second_phase_time_s=$(( 60 * 60 ))
source $script_loc/../common.sh

# Arguments:
# --system-config-file
# --physical-config-file
# --query-indexes
extract_named_arguments $@

# Just fo testing.
echo "Query indexes"
echo $ra_query_indexes
echo "rana Query Bank"
# Check file and exit if not exists
ls $ra_query_bank_file || exit 1
echo "Seq Query Bank"
ls $seq_query_bank_file || exit 1


log_workload_point "clients_starting"
# 12 clients.
start_repeating_olap_runner 12 5 2 $ra_query_indexes "ra_8" 20
rana_pid=$runner_pid

start_txn_runner_serial 20  # Implicit: --dataset-type
txn_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

# Sleep for 10 minutes and then change the SLOs.
sleep $total_first_phase_time_s

# No SLO Change on Baselines.
log_workload_point "second_phase_no_slo_change"

# Wait another hour before stopping.
sleep $total_second_phase_time_s

# Shut down everything now.
log_workload_point "experiment_workload_done"
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid
log_workload_point "shutdown_complete"
