#! /bin/bash

if [ -z $1 ]; then
  >&2 echo "Usage: $0 path/to/physical/config.yml"
  exit 1
fi

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

python3 ../../../workloads/IMDB_extended/set_up_starting_blueprint.py \
  --schema-name chbenchmark \
  --query-bank-file ../../../workloads/chbenchmark/queries.sql \
  --redshift-queries "0,1,2,3,5,6,7,8,10,11,12,13,14,15,16,18,19,20,21" \
  --athena-queries "4,9,17" \
  --redshift-provisioning "dc2.large:16" \
  --aurora-provisioning "db.r6g.2xlarge:1" \
  --system-config-file ch_scale_down_config.yml \
  --physical-config-file $1 \
  --override-definite-routing redshift
