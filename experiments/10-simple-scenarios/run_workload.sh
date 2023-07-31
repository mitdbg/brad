#! /bin/bash

python3 txn_runner.py $@ &
txn_pid=$!

python3 ana_runner.py $@ &
ana_pid=$!
wait $ana_pid

# Send SIGINT to the transaction runner process.
kill -INT $txn_pid
wait $txn_pid
