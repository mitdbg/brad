#!/bin/bash

# cd ~/TPC-Hv3.0.1/dbgen
# for q in `seq 1 22`;do DSS_QUERY=./queries ./qgen $q > $q.sql;done;
# for i in {1..22}; do cat "${i}.sql" >> q.sql; done
# mv *.sql ~/brad/sandbox/qe/queries/
# cd ~

if [ $# -gt 0 ]; then
  # generate data for specified Scale Factor
  cd ~/TPC-Hv3.0.1/dbgen
  ./dbgen -f -s $1
  mv *.tbl /tmp/tpcdata
  cd ~

  docker stop postgres
  docker rm postgres

  docker run -v ~/brad/sandbox/qe/tpch:/data -v /tmp/tpcdata:/tpcdata -m 50g --name postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres

  sleep 10
  docker exec -it postgres psql -U postgres -c 'create database tpch;'
  docker exec -it postgres psql -U postgres -d 'tpch' -c '\i /data/load_tpch.sql'
fi


# time { 
#     for q in `seq 1 22`;do docker exec -ti postgres psql -U postgres -d tpch -o /dev/null -c '\i /data/queries/'$q'.sql' | cat; done;
# }

