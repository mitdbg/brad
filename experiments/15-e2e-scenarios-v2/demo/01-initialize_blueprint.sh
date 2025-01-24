#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH

python3 set_up_starting_blueprint.py \
  --physical-config-file ../../../config/physical_config_100gb_demo.yml \
  --system-config-file ../scale_up/scale_up_config.yml \
  --schema-name imdb_extended_100g \
  --query-bank-file ../../../workloads/IMDB_100GB/regular_test/queries.sql
