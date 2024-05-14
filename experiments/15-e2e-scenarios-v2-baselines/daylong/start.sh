script_loc=$(cd $(dirname $0) && pwd -P)
bash $script_loc/run_workload.sh \
    --ra-query-bank-file="workloads/IMDB_100GB/regular_test/queries.sql"
