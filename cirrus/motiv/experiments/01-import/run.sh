#! /bin/bash

../../build/workloads/bench_import \
  --dsn=Redshift \
  --user=awsuser \
  --pwdvar=DBPWD \
  --iam_role="$RDSIAM" \
  $@ > $COND_OUT/results.csv
