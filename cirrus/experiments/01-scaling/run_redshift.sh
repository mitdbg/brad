#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

../../build/tpch_scale \
  --host=$RDS_HOST \
  --user=$RDS_USER \
  --dbname=$RDS_DBNAME \
  --pwdvar=$RDS_PWDVAR \
  $@ > $COND_OUT/results.csv
