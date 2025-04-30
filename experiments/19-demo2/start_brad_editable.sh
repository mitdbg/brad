#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc

pushd ../.. > /dev/null

brad daemon \
  --system-config config/system_config_demo2.yml \
  --physical-config config/physical_config_100gb_demo.yml \
  --schema-name imdb_editable_100g \
  --ui
