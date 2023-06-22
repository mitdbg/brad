#! /bin/bash

set -e
pytest \
  --ignore=tests/test_monitor.py \
  --ignore=tests/test_workload.py
