#! /bin/bash

# Used to copy TPC-H generated data to S3.

if [ -z $2 ]; then
  echo "Usage: $0 path/to/generated/data scale_factor_folder"
  exit 1
fi

files=(
  "customer.tbl"
  "lineitem.tbl"
  "nation.tbl"
  "orders.tbl"
  "partsupp.tbl"
  "part.tbl"
  "region.tbl"
  "supplier.tbl"
)

for f in "${files[@]}"; do
  aws s3 cp $1/$f s3://geoffxy-research/tpch/sf$2/$f
done

