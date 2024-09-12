#! /bin/bash

SCRIPT_PATH=$(cd $(dirname $0) && pwd -P)
cd $SCRIPT_PATH
source shared.sh

# NOTE: You need to manually add `# type: ignore` to the .pyi stubs, since the
# generated code causes an error in mypy's type check.
python3 -m grpc_tools.protoc \
  -I../proto \
  --python_out=../src/brad/proto_gen \
  --pyi_out=../src/brad/proto_gen \
  ../proto/interface.proto \
  ../proto/interface/blueprint.proto \
  ../proto/interface/schema.proto \
  ../proto/interface/vdbe.proto

# Fix the import path.
sed -i -e "s/from interface import/from brad.proto_gen.interface import/g" ../src/brad/proto_gen/interface_pb2.py

python3 -m grpc_tools.protoc \
  -I../proto \
  --python_out=../src/brad/proto_gen \
  --pyi_out=../src/brad/proto_gen \
  --grpc_python_out=../src/brad/proto_gen \
  ../proto/brad.proto

# Fix the import path.
sed -i -e "s/import brad_pb2 as brad__pb2/import brad.proto_gen.brad_pb2 as brad__pb2/g" ../src/brad/proto_gen/brad_pb2_grpc.py

# NOTE: You need to manually add `# type: ignore` to the `DataLocation` .pyi
# stubs, since the generated code causes an error in mypy's type check.
python3 -m grpc_tools.protoc \
  -I../proto \
  --python_out=../src/brad/proto_gen \
  --pyi_out=../src/brad/proto_gen \
  ../proto/blueprint.proto
