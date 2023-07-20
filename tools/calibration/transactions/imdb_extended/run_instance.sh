#! /bin/bash

>&2 echo "Running the transactional workload..."
python3 ../../../workloads/IMDB_extended/run_transactions.py --cstr-var $BRAD_CSTR_VAR $@

>&2 echo "Waiting 10 seconds before retrieving metrics..."
sleep 10

>&2 echo "Retrieving metrics..."
python3 retrieve_metrics.py --out-file $COND_OUT/pi_metrics.csv --instance-id $BRAD_INSTANCE_ID
