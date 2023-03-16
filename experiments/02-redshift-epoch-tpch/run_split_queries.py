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
  lineitem_merged
WHERE
  l_shipdate <= date '1998-09-02'
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
    customer_merged,
    orders_merged,
    lineitem_merged,
    supplier_merged,
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


QUERY_3 = """
  SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
  FROM
    customer_merged,
    orders_merged,
    lineitem_merged
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


QUERY_1_BARE = """
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
  lineitem
WHERE
  l_shipdate <= date '1998-09-02'
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus
"""


QUERY_5_BARE = """
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


QUERY_3_BARE = """
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


def main():
    try:
        import conductor.lib as cond

        out_dir = cond.get_output_path()
    except (ImportError, RuntimeError):
        out_dir = pathlib.Path(".")

    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument("--server", type=str, required=True)
    parser.add_argument("--db", type=str, default="iohtap")
    parser.add_argument("--port", type=int, default=5439)
    parser.add_argument("--exp", type=str)
    args = parser.parse_args()

    run_bare = args.exp == "run_bare"

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

    if run_bare:
        print("> Running Q1 bare...", file=sys.stderr, flush=True)
        for _ in range(args.trials):
            start = time.time()
            cursor.execute(QUERY_1_BARE)
            end = time.time()
            print("q1,bare,{}".format(end - start), file=out_file, flush=True)

        print("> Running Q3 bare...", file=sys.stderr, flush=True)
        for _ in range(args.trials):
            start = time.time()
            cursor.execute(QUERY_3_BARE)
            end = time.time()
            print("q3,bare,{}".format(end - start), file=out_file, flush=True)

        print("> Running Q5 bare...", file=sys.stderr, flush=True)
        for _ in range(args.trials):
            start = time.time()
            cursor.execute(QUERY_5_BARE)
            end = time.time()
            print("q5,bare,{}".format(end - start), file=out_file, flush=True)

    else:
        print("> Running Q1 merged...", file=sys.stderr, flush=True)
        for _ in range(args.trials):
            start = time.time()
            cursor.execute(QUERY_1)
            end = time.time()
            print("q1,merged,{}".format(end - start), file=out_file, flush=True)

        print("> Running Q3 merged...", file=sys.stderr, flush=True)
        for _ in range(args.trials):
            start = time.time()
            cursor.execute(QUERY_3)
            end = time.time()
            print("q3,merged,{}".format(end - start), file=out_file, flush=True)

        print("> Running Q5 merged...", file=sys.stderr, flush=True)
        for _ in range(args.trials):
            start = time.time()
            cursor.execute(QUERY_5)
            end = time.time()
            print("q5,merged,{}".format(end - start), file=out_file, flush=True)

    out_file.close()


if __name__ == "__main__":
    main()
