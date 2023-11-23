#! /bin/bash

set -e

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH

if [ -z $4 ]; then
  echo "Usage: $0 queries_json out_dir config_file schema_name"
  exit 1
fi

queries_json=$1
out_dir=$2
config_file=$3
schema_name=$4

mkdir -p $out_dir/sql
mkdir -p $out_dir/raw

for epoch in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22; do
    echo "Processing $epoch"
    sql_file_orig=$out_dir/sql/epoch_${epoch}_orig.sql
    sql_file_clean=$out_dir/sql/epoch_${epoch}.sql

    jq -r '.["epoch_'"$epoch"'"] | .[]' $queries_json > $sql_file_orig

    # Need to fix the query: `movie_telemetry` should be `telemetry`
    sed 's/movie_telemetry/telemetry/g' $sql_file_orig > $sql_file_clean

    # Need to fix problematic SQL synatx.
    sed -i 's/timestamp >/"timestamp" > timestamp/g' $sql_file_clean
    sed -i 's/timestamp </"timestamp" < timestamp/g' $sql_file_clean
    sed -i 's/timestamp <=/"timestamp" <= timestamp/g' $sql_file_clean
    sed -i 's/timestamp >=/"timestamp" >= timestamp/g' $sql_file_clean
    sed -i 's/timestamp =/"timestamp" = timestamp/g' $sql_file_clean

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
done
