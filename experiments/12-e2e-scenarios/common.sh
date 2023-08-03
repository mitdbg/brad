function start_brad() {
  config_file=$1

  pushd ../../
  BRAD_PERSIST_BLUEPRINT=1 brad daemon \
    --config-file $config_file \
    --schema-name imdb_extended \
    --planner-config-file config/planner.yml \
    --temp-config-file config/temp_config.yml \
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
