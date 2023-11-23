#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH

if [ -z $4 ]; then
  echo "Usage: $0 queries_json out_dir config_file schema_name"
fi

queries_json=$1
out_dir=$2
config_file=$3
schema_name=$4

mkdir -p $out_dir/sql
mkdir -p $out_dir/raw

for epoch in "0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22"; do
    echo "Processing $epoch"
    jq -r '.["epoch_'"$epoch"'"] | .[]' $queries_json > $out_dir/sql/epoch_${epoch}.sql

    echo "Gathering data..."
    python ../../run_cost_model.py \
      --run_workload \
      --run_workload_rank 0 \
      --run_workload_world_size 1 \
      --database athena \
      --db_name imdb_specialized_100g \
      --query_timeout 300 \
      --s3_output_path "s3://geoffxy-research/athena/out" \
      --source $out_dir/sql/epoch_${epoch}.sql \
      --target $out_dir/raw/athena_epoch_${epoch}.json

    # Expand the table.
    echo "Expanding the table for the next epoch..."
    python3 ../load_telemetry.py \
        --config-file $config_file \
        --engines athena \
        --data-s3-bucket geoffxy-research \
        --data-s3-path imdb_specialized_100g/telemetry/telemetry.csv \
        --times 1 \
        --schema-name $schema_name
fi

