script_loc=$(cd $(dirname $0) && pwd -P)
export vector_query_bank_file="$script_loc/vector.sql"
export ra_query_bank_file="workloads/IMDB_100GB/regular_test/queries.sql"
bash $script_loc/run_workload.sh