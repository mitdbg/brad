import argparse
import os
import pyodbc
import sys
import time
import pathlib


CONN_STR_TEMPLATE = "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};Schema={};"


QUERY_5 = """
  SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
  FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
  WHERE
   c_custkey = o_custkey
   AND l_orderkey = o_orderkey
   AND l_suppkey = s_suppkey
   AND c_nationkey = s_nationkey
   AND s_nationkey = n_nationkey
   AND n_regionkey = r_regionkey
   AND r_name = 'ASIA'
   AND o_orderdate >= date '1994-01-01'
   AND o_orderdate < date '1995-01-01'
 GROUP BY
  n_name
"""


QUERY_5_ICEBERG = """
  SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
  FROM
    customer_iceberg,
    orders_iceberg,
    lineitem_iceberg,
    supplier_iceberg,
    nation_iceberg,
    region_iceberg
  WHERE
   c_custkey = o_custkey
   AND l_orderkey = o_orderkey
   AND l_suppkey = s_suppkey
   AND c_nationkey = s_nationkey
   AND s_nationkey = n_nationkey
   AND n_regionkey = r_regionkey
   AND r_name = 'ASIA'
   AND o_orderdate >= date '1994-01-01'
   AND o_orderdate < date '1995-01-01'
 GROUP BY
  n_name
"""


QUERY_3 = """
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
  FROM
    customer,
    orders,
    lineitem
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15'
    AND l_shipdate > date '1995-03-15'
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority;
"""


QUERY_3_ICEBERG = """
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
  FROM
    customer_iceberg,
    orders_iceberg,
    lineitem_iceberg
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15'
    AND l_shipdate > date '1995-03-15'
  GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority;
"""


def main():
    try:
        import conductor.lib as cond
        out_dir = cond.get_output_path()
    except (ImportError, RuntimeError):
        out_dir = pathlib.Path(".")

    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", type=str, required=True)
    parser.add_argument("--athena-out-path", type=str, default="athena/out/")
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()

    aws_key = os.environ["AWS_KEY"]
    aws_secret_key = os.environ["AWS_SECRET_KEY"]

    conn_str = CONN_STR_TEMPLATE.format(
        "Athena",
        "us-east-1",
        "s3://{}/{}".format(args.s3_bucket, args.athena_out_path),
        aws_key,
        aws_secret_key,
        "iohtap",
    )
    conn = pyodbc.connect(conn_str)
    print("> Successfully connected.", file=sys.stderr, flush=True)
    cursor = conn.cursor()

    out_file = open(out_dir / "results.csv", "w")
    print("query,table_type,run_time_s", file=out_file, flush=True)
    print("> Running Q3...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_3)
        end = time.time()
        print("q3,csv,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q3 Iceberg...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_3_ICEBERG)
        end = time.time()
        print("q3,iceberg,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q5...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_5)
        end = time.time()
        print("q5,csv,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q5 Iceberg...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_5_ICEBERG)
        end = time.time()
        print("q5,iceberg,{}".format(end - start), file=out_file, flush=True)
    out_file.close()


if __name__ == "__main__":
    main()
