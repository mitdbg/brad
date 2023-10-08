#! /bin/bash

>&2 echo "Running the transactional workload..."
if [ -z $BRAD_CSTR_VAR ]; then
  python3 ../../../../workloads/IMDB_extended/run_transactions.py --config-file $BRAD_CONFIG_FILE --brad-direct $@
else
  python3 ../../../../workloads/IMDB_extended/run_transactions.py --cstr-var $BRAD_CSTR_VAR $@
fi

>&2 echo "Waiting 10 seconds before retrieving metrics..."
sleep 10

>&2 echo "Retrieving metrics..."
if [ -z $BRAD_CONFIG_FILE ]; then
  python3 retrieve_metrics.py --out-file $COND_OUT/pi_metrics.csv --instance-id $BRAD_INSTANCE_ID
else
  python3 retrieve_metrics.py --out-file $COND_OUT/pi_metrics.csv --config-file $BRAD_CONFIG_FILE
fi
