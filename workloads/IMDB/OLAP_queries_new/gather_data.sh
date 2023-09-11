#! /bin/bash

if [ -z $4 ]; then
  >&2 echo "Usage: $0 engine host user password"
  exit 1
fi

# Make sure we kill the python3 subprocess as well if this script is terminated.
# https://stackoverflow.com/questions/360201/how-do-i-kill-background-processes-jobs-when-my-shell-script-exits
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

engine=$1
dbhost=$2
dbuser=$3
dbpass=$4

dbname=imdb_extended_20g
rank=0
world_size=1

if [ $engine = "redshift" ]; then
  dbport=5439
else
  dbport=5432
fi

pushd ../../../

python3 run_cost_model.py \
  --run_workload \
  --run_workload_rank $rank \
  --run_workload_world_size $world_size \
  --database $engine \
  --db_name $dbname \
  --query_timeout 200 \
  --repetitions_per_query 2 \
  --host $dbhost \
  --port $dbport \
  --user $dbuser \
  --password "$dbpass" \
  --source workloads/IMDB/OLAP_queries_new/all_queries_under_15_s.sql \
  --target workloads/IMDB/OLAP_queries_new/data/${engine}_${dbname}_all_queries_under_15_s.json
