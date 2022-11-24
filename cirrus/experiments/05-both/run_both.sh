#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc
source ../experiment_config.sh

../../build/store_sale \
  --pg_odbc_dsn="$PG_ODBC_DSN" \
  --pg_user=$PG_ODBC_USER \
  --redshift_odbc_dsn="$RSFT_ODBC_DSN" \
  --redshift_user="$RSFT_USER" \
  --redshift_iam_role="$RSFT_IAM_ROLE" \
  --pwdvar=$PWDVAR \
  --read_db=redshift \
  $@ > $COND_OUT/results.csv
