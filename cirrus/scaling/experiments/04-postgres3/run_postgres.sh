#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

# Repeat experiment 3 times
for i in $(seq 1 3); do
  ../../build/store_sale \
    --pg_odbc_dsn="$PG_ODBC_DSN" \
    --pg_user=$PG_ODBC_USER \
    --pwdvar=$PWDVAR \
    $@ > $COND_OUT/results-$i.csv
done
