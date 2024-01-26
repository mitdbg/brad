#! /bin/bash

config_file=$1
schema_name=$2
trace_manifest=$3
issue_slots=$4

python3 ../../../workloads/IMDB_extended/run_timestamped_trace.py \
  --trace-manifest $trace_manifest \
  --issue-slots $issue_slots \
  --brad-direct \
  --config-file $config_file \
  --schema-name $schema_name \
  --engine redshift
