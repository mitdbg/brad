#!/bin/bash

# cd ~/TPC-Hv3.0.1/dbgen
# for q in `seq 1 22`;do DSS_QUERY=./queries ./qgen $q > $q.sql;done;
# for i in {1..22}; do cat "${i}.sql" >> q.sql; done
# mv *.sql ~/brad/sandbox/qe/queries/
# cd ~

NUMRUNS=3

total_time=0

if [ $# -gt 0 ]; then
  cd ~
  sleep 10
  docker exec -ti postgres psql -U postgres -d tpch -o /dev/null -c '\i /data/queries/'$1'.sql' | cat
  for i in `seq 1 $NUMRUNS`;
  do
    start=$(date +%s.%N); # Capture the start time with nanoseconds as a decimal
    docker exec postgres psql -U postgres -d tpch -o /dev/null -c '\i /data/queries/'$1'.sql' | cat;
    end=$(date +%s.%N); # Capture the end time with nanoseconds as a decimal
    elapsed=$(awk "BEGIN{print $end - $start}"); # Calculate the total elapsed time in seconds with awk
    total_time=$(awk "BEGIN{print $total_time + $elapsed}");
    echo "$1,$elapsed"
  done;
  average=$(awk "BEGIN{printf \"%.3f\", ($total_time / $NUMRUNS)}") # Calculate the average and format to 3 decimal places
  # echo "Average time to run query "$1": "$average" seconds"
fi
