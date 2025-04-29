#! /bin/bash

script_loc=$(cd $(dirname $0) && pwd -P)
cd $script_loc

pushd ../15-e2e-scenarios-v2/demo > /dev/null

./02-start_workload.sh
