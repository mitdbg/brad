alias python=python3.11
duration=600
txnengine="aurora"
analyticsengine="redshift"
echo ${analyticsengine}_expts/ana1
echo "Running txns with $1 clients"
python workloads/IMDB_extended/run_transactions.py --$txnengine --output-dir ${txnengine}_expts/txn1 --num-clients $1 --run-for-s $duration
echo "Running txns with $2 clients"
python workloads/IMDB_extended/run_transactions.py --$txnengine --output-dir ${txnengine}_expts/txn10 --num-clients $2 --run-for-s $duration

# echo "Running analytics with one client"
# python workloads/IMDB_extended/run_analytics.py --$analyticsengine --output-dir ${analyticsengine}_expts/ana1 --num-clients 1 --avg-gap-s 30 --avg-gap-std-s 5 &
# pid=$!
# echo "Waiting for analytics"
# sleep $duration
# kill -INT $pid
# wait $pid


# echo "Running analytics with three client"
# python workloads/IMDB_extended/run_analytics.py --$analyticsengine --output-dir ${analyticsengine}_expts/ana3 --num-clients 3 --avg-gap-s 3 --avg-gap-std-s 1 &
# pid=$!
# echo "Waiting for analytics"
# sleep $duration
# kill -INT $pid
# wait $pid


