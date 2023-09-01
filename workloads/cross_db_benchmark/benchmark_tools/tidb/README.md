# TIDB Comparison
### Seting up a Database
1. Go to [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-7xendfnh6ykg2) and follow the instructions to link to TIDB Cloud.
2. Create a serverless or dedicated cluster.
3. On the cluster's overview page, click on `Connect` to see connection information.
4. Copy `config/tidb.sample.yml` into `config/tidb.yml` and fill in connection information.

### Scraping Pricing Information
There does not seem to be a good way to programmatically get TIDB Pricing Information.
So we have two options:
1. Manual: Just check the "Request Units" and "Storage Size" after each phase of the experiment, and linearly interpolate.
2. Hacky. I'm not sure how to automate this (would require reading the JWT from the browser's cookies).
    * If you click on Network tab of "Inspect" in your browser, search for "aws details", you will see a GET request.
    * Right click and copy the cURL.
    * Paste into [https://curlconverter.com/](cURL converter).
    * Use the resulting python code.



### Loading Data and Querying
Assuming data has already been generated into csv files. 
```sh
# Loading data.
python run_tidb.py (--data_dir imdb) (--dataset imdb_extended)
# Forcibly reloading in case of an error
python run_tidb.py --force_load

# Sending an individual query
python run_tidb.py --run_query "SELECT COUNT(*) FROM title"

# Running workloads.
## For transactions.
python workloads/IMDB_extended/run_transactions.py --tidb

## For analytics.
python workloads/IMDB_extended/run_analytics.py --tidb
```

### TODOs
* TiDB Serverless fails on many queries. Figure out why.