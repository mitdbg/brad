script_loc=$(cd $(dirname $0) && pwd -P)
bash $script_loc/run_workload_txn_up.sh \
    --ra-query-bank-file="workloads/IMDB_100GB/regular_test/queries.sql"

bash $script_loc/run_workload_ana_up.sh \
    --ra-query-bank-file="workloads/IMDB_100GB/regular_test/queries.sql"

# bash $script_loc/run_workload_txn_ana_up.sh \
#     --ra-query-bank-file="workloads/IMDB_100GB/regular_test/queries.sql"
