#! /bin/bash

python3 ana_runner.py $@ &
ana_pid=$!

python3 txn_runner.py $@ &
txn_pid=$!

wait $ana_pid
wait $txn_pid
