#! /bin/bash

# Make sure we kill the python3 subprocess as well if this script is terminated.
# https://stackoverflow.com/questions/360201/how-do-i-kill-background-processes-jobs-when-my-shell-script-exits
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

engine=athena

dbname=imdb_extended_20g
rank=0
world_size=1

pushd ../../../

python run_cost_model.py \
  --run_workload \
  --run_workload_rank $rank \
  --run_workload_world_size $world_size \
  --database athena \
  --db_name $dbname \
  --query_timeout 200 \
  --source workloads/IMDB/OLAP_queries_new/all_queries_under_15_s.sql \
  --target workloads/IMDB/OLAP_queries_new/data/${engine}_${dbname}_all_queries_under_15_s.json
