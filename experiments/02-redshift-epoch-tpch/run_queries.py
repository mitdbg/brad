import argparse
import os
import pyodbc
import sys
import time
import pathlib


CONN_STR_TEMPLATE = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database={};"


QUERY_1 = """
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (1-l_discount)) AS sum_disc_price,
  SUM(l_extendedprice * (1-l_discount) * (1+l_tax)) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  lineitem_010
WHERE
  l_shipdate <= date '1998-09-02'
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus
"""


QUERY_1_FILTERS = """
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (1-l_discount)) AS sum_disc_price,
  SUM(l_extendedprice * (1-l_discount) * (1+l_tax)) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM
  lineitem_010
WHERE
  l_shipdate <= date '1998-09-02'
  AND l_epoch_start >= 0
  AND l_epoch_end < 10000
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus
"""


QUERY_5 = """
  SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
  FROM
    customer_010,
    orders_010,
    lineitem_010,
    supplier_010,
    nation_010,
    region_010
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


QUERY_5_FILTERS = """
  SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
  FROM
    customer_010,
    orders_010,
    lineitem_010,
    supplier_010,
    nation_010,
    region_010
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
   AND c_epoch_start >= 0
   AND c_epoch_end < 10000
   AND o_epoch_start >= 0
   AND o_epoch_end < 10000
   AND l_epoch_start >= 0
   AND l_epoch_end < 10000
   AND s_epoch_start >= 0
   AND s_epoch_end < 10000
   AND n_epoch_start >= 0
   AND n_epoch_end < 10000
   AND r_epoch_start >= 0
   AND r_epoch_end < 10000
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
    customer_010,
    orders_010,
    lineitem_010
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


QUERY_3_FILTERS = """
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
  FROM
    customer_010,
    orders_010,
    lineitem_010
  WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < date '1995-03-15'
    AND l_shipdate > date '1995-03-15'
    AND c_epoch_start >= 0
    AND c_epoch_end < 10000
    AND o_epoch_start >= 0
    AND o_epoch_end < 10000
    AND l_epoch_start >= 0
    AND l_epoch_end < 10000
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
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--server", type=str, required=True)
    parser.add_argument("--db", type=str, default="dev")
    parser.add_argument("--port", type=int, default=5439)
    args = parser.parse_args()

    rds_uid = os.environ["RDS_UID"]
    rds_pwd = os.environ["RDS_PWD"]

    conn_str = CONN_STR_TEMPLATE.format(
        "Amazon Redshift (x64)",
        args.server,
        args.port,
        rds_uid,
        rds_pwd,
        args.db,
    )
    conn = pyodbc.connect(conn_str)
    print("> Successfully connected.", file=sys.stderr, flush=True)
    cursor = conn.cursor()

    # Disable the result cache
    cursor.execute("SET enable_result_cache_for_session = off")
    print("> Disabled the result cache", file=sys.stderr)

    out_file = open(out_dir / "results.csv", "w")
    print("query,type,run_time_s", file=out_file, flush=True)
    print("> Running Q1...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_1)
        end = time.time()
        print("q1,bare,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q1 filtered...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_1_FILTERS)
        end = time.time()
        print("q1,filtered,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q3...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_3)
        end = time.time()
        print("q3,bare,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q3 filtered...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_3_FILTERS)
        end = time.time()
        print("q3,filtered,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q5...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_5)
        end = time.time()
        print("q5,bare,{}".format(end - start), file=out_file, flush=True)

    print("> Running Q5 Iceberg...", file=sys.stderr, flush=True)
    for _ in range(args.trials):
        start = time.time()
        cursor.execute(QUERY_5_FILTERS)
        end = time.time()
        print("q5,filtered,{}".format(end - start), file=out_file, flush=True)
    out_file.close()


if __name__ == "__main__":
    main()
