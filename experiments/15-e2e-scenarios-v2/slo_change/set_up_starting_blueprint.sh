#! /bin/bash

if [ -z $1 ]; then
  >&2 echo "Usage: $0 path/to/physical/config.yml"
  exit 1
fi

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../common.sh

python3 ../../../workloads/IMDB_extended/set_up_starting_blueprint.py \
  --schema-name imdb_extended_100g \
  --query-bank-file ../../../workloads/IMDB_100GB/regular_test/queries.sql \
  --aurora-queries "99,56,32,92,91" \
  --redshift-queries "49,30,83,94,38,87,86,76,37,31,46,58,61,62,64,69,73,74,51,57,60" \
  --redshift-provisioning "dc2.large:2" \
  --aurora-provisioning "db.r6g.xlarge:2" \
  --system-config-file slo_change_config.yml \
  --physical-config-file $1
