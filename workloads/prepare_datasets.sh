#! /bin/bash

# This script is used to prepare the training datasets for run time and data
# scanned. We want one entrypoint to avoid making errors.

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc

if [ -z $5 ]; then
  >&2 echo "Usage: $0 out_name dataset_dir_prefix athena_parsed aurora_parsed redshift_parsed"
fi

out_name=$1
# Assumes the dataset is stored at <prefix>_train and <prefix>_test
dataset_dir_prefix=$2
athena_parsed=$3
aurora_parsed=$4
redshift_parsed=$5

set -e

# 1. Make the output directory.
out_dir=$PREFIX/$out_name
mkdir -p $out_dir

# 2. Run the train/test split.
function split_parsed() {
  python3 ../tools/query_dataset/split_parsed.py \
    --source-parsed $1 \
    --queries-file $2 \
    --out-file-1 $3
}

mkdir $out_dir/run_time

split_parsed $athena_parsed ${dataset_dir_prefix}_train/queries.sql $out_dir/run_time/athena_${out_name}_train.json
split_parsed $athena_parsed ${dataset_dir_prefix}_test/queries.sql $out_dir/run_time/athena_${out_name}_test.json

split_parsed $aurora_parsed ${dataset_dir_prefix}_train/queries.sql $out_dir/run_time/aurora_${out_name}_train.json
split_parsed $aurora_parsed ${dataset_dir_prefix}_test/queries.sql $out_dir/run_time/aurora_${out_name}_test.json

split_parsed $redshift_parsed ${dataset_dir_prefix}_train/queries.sql $out_dir/run_time/redshift_${out_name}_train.json
split_parsed $redshift_parsed ${dataset_dir_prefix}_test/queries.sql $out_dir/run_time/redshift_${out_name}_test.json

# 3. Clean up the data.
function fix_missing_rt() {
  python3 ../tools/one_off/fix_missing_run_times.py --in-file $1
}
find "$out_dir/run_time" -type f -name "*.json" | xargs fix_missing_rt

pushd $out_dir/run_time
rm *.orig
popd

# 3. Run run time data augmentation.
mkdir $out_dir/run_time_augmented

function run_augmentation() {
  python3 ../run_cost_model.py --argment_dataset --workload_runs $2 --target $1
}

find "$out_dir/run_time" -type f -name "*_train.json" | xargs run_augmentation $out_dir/run_time_augmented
