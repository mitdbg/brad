#! /bin/bash

set -e
pytest \
  --ignore=tests/test_workload.py
