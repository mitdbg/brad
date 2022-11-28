#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

../../build/tpch_scale \
  --db="aurora" \
  --user=$AUR_USER \
  --pwdvar=$AUR_PWDVAR \
  $@ > $COND_OUT/results.csv
