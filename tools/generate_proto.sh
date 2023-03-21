#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH
source shared.sh

python3 -m grpc_tools.protoc \
  -I../proto \
  --python_out=../src/brad/grpc_gen \
  --pyi_out=../src/brad/grpc_gen \
  --grpc_python_out=../src/brad/grpc_gen \
  ../proto/brad.proto

# Fix the import path.
sed -i -e "s/import brad_pb2 as brad__pb2/import brad.grpc_gen.brad_pb2 as brad__pb2/g" ../src/brad/grpc_gen/brad_pb2_grpc.py
