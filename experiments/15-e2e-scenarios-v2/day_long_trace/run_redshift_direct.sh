#! /bin/bash

config_file=$1
schema_name=$2

python3 ../../../workloads/IMDB_extended/run_timestamped_trace.py \
  --trace-manifest trace_manifest.yml \
  --issue-slots 20 \
  --brad-direct \
  --config-file $config_file \
  --schema-name $schema_name \
  --engine redshift
