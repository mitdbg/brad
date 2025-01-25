#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH

mkdir -p output

COND_OUT=./output ./full_workload_def.sh \
  --physical-config-file=../../../config/physical_config_100gb_demo.yml \
  --system-config-file=../../../config/system_config_demo.yml \
  --schema-name=imdb_extended_100g \
  --ra-query-bank-file=../../../workloads/IMDB_100GB/regular_test/queries.sql \
  --num-front-ends=8
