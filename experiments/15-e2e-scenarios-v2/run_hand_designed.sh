#! /bin/bash

# This workflow should ideally be orchestrated entirely by Conductor. We're
# currently missing a nice "sequence" abstraction, which makes this too verbose
# to express in Conductor.

# NOTE: Change the configs to your specific setup as needed.

if [ ! -z $1 ]; then
  shutdown_after=1
  echo "Will shutdown after this script completes."
fi

function scale_down() {
  # Scale down.
  echo "Running the scale down baseline."
  python3 scale_down/set_up_hand_designed.py \
    --physical-config-file ../../config/physical_config_100gb.yml \
    --schema-name imdb_extended_100g \
    --system-config-file scale_down/scale_down_config.yml \
    --query-bank-file ../../workloads/IMDB_100GB/regular_test/queries.sql

  sleep 60

  # Run the baseline
  cond run 15-e2e-scenarios-v2/scale_down/:hand_designed_100g --this-commit
  sleep 60
}

function scale_up() {
  # Scale up (transactions)
  echo "Running the scale up txn baseline."
  python3 scale_up/set_up_hand_designed_txn.py \
    --physical-config-file ../../config/physical_config_100gb.yml \
    --schema-name imdb_extended_100g \
    --system-config-file scale_up/scale_up_config.yml \
    --query-bank-file ../../workloads/IMDB_100GB/regular_test/queries.sql

  # Warm up first.
  cond run 15-e2e-scenarios-v2/scale_up/:brad_100g_warmup
  sleep 60

  # Baseline.
  cond run 15-e2e-scenarios-v2/scale_up/:hand_designed_100g_txn_up
  sleep 60
}

function specialized() {
  # Specialized (vector)
  echo "Running the specialized baseline."
  python3 specialized/set_up_vector_hand_designed.py \
    --physical-config-file ../../config/physical_config_specialized.yml \
    --schema-name imdb_specialized_100g \
    --system-config-file specialized/specialized_config.yml \
    --query-bank-file ../../workloads/IMDB_100GB/regular_test/queries.sql

  # Warm up first.
  cond run 15-e2e-scenarios-v2/specialized/:brad_100g_vector_warmup
  sleep 60

  # Baseline.
  cond run 15-e2e-scenarios-v2/specialized/:hand_designed_100g_vector
  sleep 60
}

### Regular cluster experiments.

brad admin --debug control resume --schema-name imdb_extended_100g \
  --physical-config-file ../../config/physical_config_100gb.yml \
  --system-config-file ../../config/system_config.yml

scale_down
scale_up

if [ ! -z $shutdown_after ]; then
  echo "Shutting down the regular cluster..."
  brad admin --debug control pause --schema-name imdb_extended_100g \
    --physical-config-file ../../config/physical_config_100gb.yml \
    --system-config-file ../../config/system_config.yml
  sleep 60
fi

### Specialized baseline.

brad admin --debug control resume --schema-name imdb_specialized_100g \
  --physical-config-file ../../config/physical_config_specialized.yml \
  --system-config-file ../../config/system_config.yml

specialized

if [ ! -z $shutdown_after ]; then
  echo "Shutting down the specialized cluster..."
  brad admin --debug control pause --schema-name imdb_specialized_100g \
    --physical-config-file ../../config/physical_config_specialized.yml \
    --system-config-file ../../config/system_config.yml
  sleep 60
fi

if [ ! -z $shutdown_after ]; then
  sudo shutdown now
fi
