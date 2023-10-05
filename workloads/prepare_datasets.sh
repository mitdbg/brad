#! /bin/bash

# This script is used to prepare the training datasets for run time and data
# scanned. We want one entrypoint to avoid making errors.

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc

if [ -z $5 ]; then
  >&2 echo "Usage: $0 out_name dataset_dir_prefix athena_parsed aurora_parsed redshift_parsed"
  exit 1
fi

out_name=$1
# Assumes the dataset is stored at <prefix>_train and <prefix>_test
dataset_dir_prefix=$2
athena_parsed=$3
aurora_parsed=$4
redshift_parsed=$5

set -e

# 1. Make the output directory.
PREFIX="${PREFIX:-.}"
out_dir=$PREFIX/$out_name
mkdir -p $out_dir

# 2. Run the train/test split.
function split_parsed() {
  python3 ../tools/query_dataset/split_parsed.py \
    --source-parsed $1 \
    --queries-file $2 \
    --out-file-1 $3
}

echo "--- Running train/test split ---"
mkdir $out_dir/run_time

split_parsed $athena_parsed ${dataset_dir_prefix}_train/queries.sql $out_dir/run_time/athena_${out_name}_train.json
split_parsed $athena_parsed ${dataset_dir_prefix}_test/queries.sql $out_dir/run_time/athena_${out_name}_test.json

split_parsed $aurora_parsed ${dataset_dir_prefix}_train/queries.sql $out_dir/run_time/aurora_${out_name}_train.json
split_parsed $aurora_parsed ${dataset_dir_prefix}_test/queries.sql $out_dir/run_time/aurora_${out_name}_test.json

split_parsed $redshift_parsed ${dataset_dir_prefix}_train/queries.sql $out_dir/run_time/redshift_${out_name}_train.json
split_parsed $redshift_parsed ${dataset_dir_prefix}_test/queries.sql $out_dir/run_time/redshift_${out_name}_test.json

# Extract the database stats.
python3 -c "import json; with open(\"${athena_parsed}\") as file: d = json.load(file); with open(\"${out_dir}/run_time/database_${out_name}_stats.json\", \"w\") as file: json.dump(d['database_stats'], file);"

# 3. Clean up the data.
function fix_missing_rt() {
  python3 ../tools/one_off/fix_missing_parsed_run_times.py --in-file $1
}

echo "--- Cleaning up missing run times ---"
for datafile in $(find "$out_dir/run_time" -type f -name "*.json"); do
  fix_missing_rt $datafile
done

pushd $out_dir/run_time
rm *.json_orig
popd

# 4. Run run time data augmentation.
mkdir -p $out_dir/run_time_augmented

function run_augmentation() {
  python3 ../run_cost_model.py --argment_dataset --workload_runs $2 --target $1
}

echo "--- Running data augmentation ---"
for datafile in $(find "$out_dir/run_time" -type f -name "*_train.json"); do
  run_augmentation $out_dir/run_time_augmented $datafile
done

# 5. Create data accessed stats.
mkdir -p $out_dir/data_accessed

function add_data() {
  python3 ../tools/query_dataset/add_data_accessed.py \
    --parsed-queries-file $1 \
    --all-queries-file $2 \
    --data-accessed-file $3 \
    --engine $4 \
    --out-file $5 \
    --take-log
}

# TODO: Do we want to use the augmented dataset?
# NOTE: There will be numpy warnings here, but they are safe to ignore.
echo "--- Adding data accessed stats ---"
for db in athena aurora; do
  for mode in train test; do
    add_data $out_dir/run_time/${db}_${out_name}_${mode}.json \
      ${dataset_dir_prefix}_${mode}/queries.sql \
      ${dataset_dir_prefix}_${mode}/data_accessed-athena-aurora.npy \
      ${db} \
      $out_dir/data_accessed/${db}_${out_name}_${mode}.json
  done
done
