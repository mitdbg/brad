# TIDB Comparison
### Seting up a Database
1. Go to [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-7xendfnh6ykg2) and follow the instructions to link to TIDB Cloud.
2. Create a serverless or dedicated cluster.
3. On the cluster's overview page, click on `Connect` to see connection information.
4. Copy `config/tidb.sample.yml` into `config/tidb.yml` and fill in connection information.

### Loading Data and Querying
```py
# Connect using the `tidb.tml` config.
tidb = TiDB()

# Load/query using the connection (requires mysql syntax).
conn = tidb.get_connection()
cur = conn.cursor()
# Use cursor ...

# TODO: If loading from s3 is more convenient, add a `load_data` method.
```

### TODOs
* It seems you can only load large amounts of data using S3 (like Redshift).
    * So use Redshift's existing code to load data.
* Querying requires mysql syntax (existing code uses postgres).
    * Check if all queries work out of the box.
    