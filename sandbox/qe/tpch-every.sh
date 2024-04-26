#!/bin/bash

cd ~/brad/sandbox/qe
for q in `seq 1 22`;
do 
  echo "timing query $q";
  timeout --foreground 2m ./tpch-individual.sh $q
# timeout --foreground 30s ./tpch-individual.sh 2;
done;

