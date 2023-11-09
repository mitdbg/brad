script_loc=$(cd $(dirname $0) && pwd -P)
bash $script_loc/run_workload.sh \
    --ra-query-indexes="99,56,32,92,91,49,30,83,94,38,87,86,76,37,31,46" \
    --ra-query-bank-file="workloads/IMDB_100GB/regular_test/queries.sql"
