#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source common.sh

if [ -z $1 ]; then
  >&2 echo "Usage: $0 path/to/physical/config/file.yml"
  exit 1
fi

./scale_down_workload_impl.sh \
  --physical-config-file=$1 \
  --system-config-file=scale_down_config.yml \
  --schema-name=imdb_extended_100g
