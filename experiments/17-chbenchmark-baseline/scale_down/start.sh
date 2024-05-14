script_loc=$(cd $(dirname $0) && pwd -P)
export ra_query_bank_file="workloads/chbenchmark/queries_tidb.sql"
bash $script_loc/run_workload.sh \
    --ra-query-indexes="0,1,2,3,5,6,7,8,10,11,12,13,14,15,16,18,19,21" \
    --ra-query-bank-file=$ra_query_bank_file
