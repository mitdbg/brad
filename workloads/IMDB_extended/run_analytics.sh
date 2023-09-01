echo "Running analytics with one client"
python workloads/IMDB_extended/run_analytics.py --tidb --output-dir tidb_expts/ana1 --num-clients 1 --avg-gap-s 30 --avg-gap-std-s 5 &
pid=$!
echo "Waiting for analytics"
sleep 600
kill -INT $pid
wait $pid


echo "Running analytics with three client"
python workloads/IMDB_extended/run_analytics.py --tidb --output-dir tidb_expts/ana3 --num-clients 3 --avg-gap-s 3 --avg-gap-std-s 1 &
pid=$!
echo "Waiting for analytics"
sleep 600
kill -INT $pid
wait $pid
