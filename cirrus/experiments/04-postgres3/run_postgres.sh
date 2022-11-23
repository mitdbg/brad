#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

../../build/store_sale \
  --pg_odbc_dsn="$PG_ODBC_DSN" \
  --pg_user=$PG_ODBC_USER \
  --pwdvar=$PWDVAR \
  $@ > $COND_OUT/results.csv
