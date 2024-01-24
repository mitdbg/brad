#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

# Arguments:
# --config-file
# --planner-config-file
# --query-indexes
extract_named_arguments $@

start_brad $system_config_file $physical_config_file
log_workload_point "brad_start_initiated"
sleep 30

log_workload_point "clients_starting"

clients_multiplier=1
time_scale_factor=2
run_for_s=$((12 * 60 * 60))  # 12 hours.
issue_slots=5

# Repeating analytics.
start_snowset_repeating_olap_runner $((10 * $clients_multiplier)) $time_scale_factor $clients_multiplier "ra" $run_for_s $issue_slots
rana_pid=$runner_pid

# Transactions.
start_snowset_txn_runner $((10 * $clients_multiplier)) $time_scale_factor $clients_multiplier "t" $run_for_s 1  # NOTE: 1
txn_pid=$runner_pid

# Ad-hoc queries.
# 2 clients, issuing once per 8 minutes on average with a standard deviation of
# 2 minutes.
start_sequence_runner 2 $((8 * 60)) $((2 * 60)) "adhoc" 0 $issue_slots
adhoc_pid=$runner_pid

log_workload_point "clients_started"

function inner_cancel_experiment() {
  cancel_experiment $rana_pid $txn_pid $adhoc_pid
}

trap "inner_cancel_experiment" INT
trap "inner_cancel_experiment" TERM

if [ -z $is_daylong_hd ]; then
  sleep $run_for_s
else
  # Used for the hand designed baseline.
  # Note - these offsets must be adjusted if you scale the experiment run time.
  >&2 echo "[!] Running the hand designed baseline [!]"
  expt_start="$(date -u +%s)"
  sleep $((290 * 60))

  med_start="$(date -u +%s)"
  brad cli --command "BRAD_USE_PRESET_BP dl_med"
  pause_for_s_past_timepoint $med_start $((90 * 60))

  hi_start="$(date -u +%s)"
  brad cli --command "BRAD_USE_PRESET_BP dl_hi"
  pause_for_s_past_timepoint $hi_start $((120 * 60))

  med2_start="$(date -u +%s)"
  brad cli --command "BRAD_USE_PRESET_BP dl_med"
  pause_for_s_past_timepoint $med2_start $((50 * 60))

  brad cli --command "BRAD_USE_PRESET_BP dl_lo"
  pause_for_s_past_timepoint $expt_start $run_for_s
fi

sleep $((5 * 60))  # Wait for an extra 5 minutes.
log_workload_point "experiment_done"

# Shut down everything now.
>&2 echo "Experiment done. Shutting down runners..."
graceful_shutdown $rana_pid $txn_pid $adhoc_pid
log_workload_point "shutdown_complete"
