## Setup environment

Only three packages: absl-py, numpy, pandas
```angular2html
pip install -r requirements.txt
```

## Download IMDB data
```angular2html
mkdir imdb && cd imdb && wget -c http://homepages.cwi.nl/~boncz/job/imdb.tgz && tar -xvzf imdb.tgz
python workloads/IMDB/prepend_imdb_headers.py --csv_dir /path/to/imdb
```

## Execute repeating analytical queries
```angular2html
cd workloads/IMDB_extended/
python run_repeating_analytics.py --query-bank-file ../IMDB_20GB/regular_test/queries.sql --num-clients 3 --query-frequency-path ../IMDB_20GB/regular_test/query_frequency.npy --avg-gap-s 1 --query-indexes 1,3,5,7 --run-for-s 50
```

Run workload with snowset trace for one day
```angular2html
cd workloads/IMDB_extended/
python run_repeating_analytics.py --query-bank-file ../IMDB_20GB/regular_test/queries.sql --query-frequency-path ../IMDB_20GB/regular_test/query_frequency.npy --num-client-path ../IMDB_20GB/regular_test/num_client.pkl --num-clients 8 --gap-dist-path ../IMDB_20GB/regular_test/gap_time_dist.npy --time-scale-factor 200 --run-for-s 600
```