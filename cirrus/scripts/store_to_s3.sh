#! /bin/bash

# Used to generate the "store dataset" and to copy it to S3.

if [ -z $1 ]; then
  echo "Usage: $0 scale_factor"
  exit 1
fi

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc

files=(
  "inventory.tbl"
  "sales.tbl"
)

padded_sf=$(printf "%03d" $1)

echo "Generating the data..."
mkdir sf$padded_sf
../build/store_gen --sf=$1 --gen_out=sf$padded_sf --action=generate

echo "Copying it to S3..."
for f in "${files[@]}"; do
  aws s3 cp sf$padded_sf/$f s3://geoffxy-research/store/sf$padded_sf/$f
done

echo "Done!"
