#!/bin/bash

# cd ~/TPC-Hv3.0.1/dbgen
# for q in `seq 1 22`;do DSS_QUERY=./queries ./qgen $q > $q.sql;done;
# for i in {1..22}; do cat "${i}.sql" >> q.sql; done
# mv *.sql ~/brad/sandbox/qe/queries/
# cd ~

NUMRUNS=10

if [ $# -gt 0 ]; then
  cd ~
  sleep 10
  docker exec -ti postgres psql -U postgres -d tpch -o /dev/null -c '\i /data/queries/'$1'.sql' | cat
  start=$(date +%s.%N) # Capture the start time with nanoseconds as a decimal
  for i in `seq 1 $NUMRUNS`;do docker exec -ti postgres psql -U postgres -d tpch -o /dev/null -c '\i /data/queries/'$1'.sql' | cat; done;
  end=$(date +%s.%N) # Capture the end time with nanoseconds as a decimal
  elapsed=$(awk "BEGIN{print $end - $start}") # Calculate the total elapsed time in seconds with awk
  average=$(awk "BEGIN{printf \"%.3f\", ($elapsed / $NUMRUNS)}") # Calculate the average and format to 3 decimal places
  echo "Average time to run query "$1": "$average" seconds"
fi




