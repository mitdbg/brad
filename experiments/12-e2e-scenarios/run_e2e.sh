#! /bin/bash

function run_transition() {
  brad admin --debug \
    modify_blueprint \
    --config-file ../../config/config.yml \
    --schema-name imdb_extended \
    --continue-transition
}

function gather_stats() {
  brad admin run_on aurora ANALYZE \
    --config-file ../../config/config.yml \
    --schema-name imdb_extended
}

cond run //12-e2e-scenarios:light_start
run_transition
gather_stats

cond run //12-e2e-scenarios:increase_txns_1
run_transition
gather_stats

cond run //12-e2e-scenarios:increase_txns_2
run_transition
gather_stats
