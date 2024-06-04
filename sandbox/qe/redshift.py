import argparse
import os
import redshift_connector
import time


def init(cs):
    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "nation" (
    "n_nationkey"  INT,
    "n_name"       CHAR(25),
    "n_regionkey"  INT,
    "n_comment"    VARCHAR(152),
    PRIMARY KEY ("n_nationkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "region" (
    "r_regionkey"  INT,
    "r_name"       CHAR(25),
    "r_comment"    VARCHAR(152),
    PRIMARY KEY ("r_regionkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "supplier" (
    "s_suppkey"     INT,
    "s_name"        CHAR(25),
    "s_address"     VARCHAR(40),
    "s_nationkey"   INT,
    "s_phone"       CHAR(15),
    "s_acctbal"     DECIMAL(15,2),
    "s_comment"     VARCHAR(101),
    PRIMARY KEY ("s_suppkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "customer" (
    "c_custkey"     INT,
    "c_name"        VARCHAR(25),
    "c_address"     VARCHAR(40),
    "c_nationkey"   INT,
    "c_phone"       CHAR(15),
    "c_acctbal"     DECIMAL(15,2),
    "c_mktsegment"  CHAR(10),
    "c_comment"     VARCHAR(117),
    PRIMARY KEY ("c_custkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "part" (
    "p_partkey"     INT,
    "p_name"        VARCHAR(55),
    "p_mfgr"        CHAR(25),
    "p_brand"       CHAR(10),
    "p_type"        VARCHAR(25),
    "p_size"        INT,
    "p_container"   CHAR(10),
    "p_retailprice" DECIMAL(15,2) ,
    "p_comment"     VARCHAR(23) ,
    PRIMARY KEY ("p_partkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "partsupp" (
    "ps_partkey"     INT,
    "ps_suppkey"     INT,
    "ps_availqty"    INT,
    "ps_supplycost"  DECIMAL(15,2),
    "ps_comment"     VARCHAR(199),
    PRIMARY KEY ("ps_partkey", "ps_suppkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "orders" (
    "o_orderkey"       INT,
    "o_custkey"        INT,
    "o_orderstatus"    CHAR(1),
    "o_totalprice"     DECIMAL(15,2),
    "o_orderdate"      DATE,
    "o_orderpriority"  CHAR(15),
    "o_clerk"          CHAR(15),
    "o_shippriority"   INT,
    "o_comment"        VARCHAR(79),
    PRIMARY KEY ("o_orderkey"));
    """
    )

    cs.execute(
        """
    CREATE TABLE IF NOT EXISTS "lineitem"(
    "l_orderkey"          INT,
    "l_partkey"           INT,
    "l_suppkey"           INT,
    "l_linenumber"        INT,
    "l_quantity"          DECIMAL(15,2),
    "l_extendedprice"     DECIMAL(15,2),
    "l_discount"          DECIMAL(15,2),
    "l_tax"               DECIMAL(15,2),
    "l_returnflag"        CHAR(1),
    "l_linestatus"        CHAR(1),
    "l_shipdate"          DATE,
    "l_commitdate"        DATE,
    "l_receiptdate"       DATE,
    "l_shipinstruct"      CHAR(25),
    "l_shipmode"          CHAR(10),
    "l_comment"           VARCHAR(44)
    );
    """
    )
    cs.execute(
        """
    COPY nation
        FROM 's3://geoffxy-research/shared/sf1/nation.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY region
        FROM 's3://geoffxy-research/shared/sf1/region.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY supplier
        FROM 's3://geoffxy-research/shared/sf1/supplier.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY customer
        FROM 's3://geoffxy-research/shared/sf1/customer.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY part
        FROM 's3://geoffxy-research/shared/sf1/part.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY partsupp
        FROM 's3://geoffxy-research/shared/sf1/partsupp.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY orders
        FROM 's3://geoffxy-research/shared/sf1/orders.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )
    cs.execute(
        """
    COPY lineitem
        FROM 's3://geoffxy-research/shared/sf1/lineitem.csv' 
        IAM_ROLE 'arn:aws:iam::498725316081:role/service-role/AmazonRedshift-CommandsAccessRole-20230606T052021'
        CSV
        IGNOREHEADER 1;
    """
    )


def time_query(cs, query_dir, i):
    query_path = os.path.join(query_dir, f"{i}.sql")
    if not os.path.exists(query_path):
        print(f"File {query_path} does not exist.")
        return

    with open(query_path, "r") as file:
        query = file.read()
    start_time = time.time()
    cs.execute(query)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Query in file {i}.sql took {elapsed_time:.4f} seconds to execute.")


def run(cs):
    # Where TPC-H queries are located
    query_dir = "/spinning/axing/queries"

    for i in range(1, 23):
        time_query(cs, query_dir, i)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Benchmark TPC-H on Redshift. takes in a argument "init"|"run"'
    )
    parser.add_argument(
        "task",
        choices=["init", "run"],
        help="init on first time running, run to run the benchmark",
    )

    args = parser.parse_args()

    conn = redshift_connector.connect(
        host="redshift-axing.cv1pkocptzr2.us-east-1.redshift.amazonaws.com",
        database="tpch",
        port=5439,
        user="awsuser",
        password="axingUROP2024",
    )

    conn.rollback()
    conn.autocommit = True
    conn.run("VACUUM")

    cs = conn.cursor()

    cs.execute("SET enable_result_cache_for_session TO OFF")

    if args.task == "init":
        init(cs)
    elif args.task == "run":
        run(cs)
    else:
        print(f"Unknown task: {args.task}")
