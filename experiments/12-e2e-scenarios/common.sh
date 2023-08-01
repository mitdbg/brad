function start_brad() {
  pushd ../../
  brad daemon \
    --config-file config/config_cond.yml \
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
