script_loc=$(cd $(dirname $0) && pwd -P)
export seq_query_bank_file="workloads/IMDB_100GB/adhoc_test/queries.sql"
export ra_query_bank_file="workloads/IMDB_100GB/regular_test/queries.sql"
bash $script_loc/run_workload.sh \
    --ra-query-indexes="99,56,32,92,91,49,30,83,94,87,86,76,37,31,46" \
    --ra-query-bank-file=$ra_query_bank_file
