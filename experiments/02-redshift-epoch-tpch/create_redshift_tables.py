import argparse
import pyodbc
import sys
import os

CONN_STR_TEMPLATE = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database={};"

TABLES = [
    {
        "name": "part",
        "create_cols": (
            "("
            "  p_partkey  INTEGER PRIMARY KEY,"
            "  p_name     VARCHAR(55),"
            "  p_mfgr     CHAR(25),"
            "  p_brand    CHAR(10),"
            "  p_type     VARCHAR(25),"
            "  p_size     INTEGER,"
            "  p_container    CHAR(10),"
            "  p_retailprice  DECIMAL,"
            "  p_comment  VARCHAR(23),"
            "  p_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "supplier",
        "create_cols": (
            "("
            "  s_suppkey   INTEGER PRIMARY KEY,"
            "  s_name			CHAR(25),"
            "  s_address   VARCHAR(40),"
            "  s_nationkey BIGINT NOT NULL,"
            "  s_phone     CHAR(15),"
            "  s_acctbal   DECIMAL,"
            "  s_comment   VARCHAR(101),"
            "  s_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "partsupp",
        "create_cols": (
            "("
            "  ps_partkey     BIGINT NOT NULL,"
            "  ps_suppkey     BIGINT NOT NULL,"
            "  ps_availqty    INTEGER,"
            "  ps_supplycost  DECIMAL,"
            "  ps_comment     VARCHAR(199),"
            "  ps_extra  CHAR(1),"
            "  PRIMARY KEY (ps_partkey, ps_suppkey)"
            ")"
        ),
    },
    {
        "name": "customer",
        "create_cols": (
            "("
            "  c_custkey    INTEGER PRIMARY KEY,"
            "  c_name       VARCHAR(25),"
            "  c_address    VARCHAR(40),"
            "  c_nationkey  BIGINT NOT NULL,"
            "  c_phone      CHAR(15),"
            "  c_acctbal    DECIMAL,"
            "  c_mktsegment CHAR(10),"
            "  c_comment    VARCHAR(117),"
            "  c_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "orders",
        "create_cols": (
            "("
            "  o_orderkey     INTEGER PRIMARY KEY,"
            "  o_custkey      BIGINT NOT NULL,"
            "  o_orderstatus  CHAR(1),"
            "  o_totalprice	  DECIMAL,"
            "  o_orderdate    DATE,"
            "  o_orderpriority	CHAR(15),"
            "  o_clerk        CHAR(15),"
            "  o_shippriority INTEGER,"
            "  o_comment      VARCHAR(79),"
            "  o_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "lineitem",
        "create_cols": (
            "("
            "  l_orderkey       BIGINT NOT NULL,"
            "  l_partkey        BIGINT NOT NULL,"
            "  l_suppkey        BIGINT NOT NULL,"
            "  l_linenumber     INTEGER,"
            "  l_quantity       DECIMAL,"
            "  l_extendedprice	DECIMAL,"
            "  l_discount       DECIMAL,"
            "  l_tax            DECIMAL,"
            "  l_returnflag     CHAR(1),"
            "  l_linestatus     CHAR(1),"
            "  l_shipdate       DATE,"
            "  l_commitdate     DATE,"
            "  l_receiptdate    DATE,"
            "  l_shipinstruct	  CHAR(25),"
            "  l_shipmode       CHAR(10),"
            "  l_comment        VARCHAR(44),"
            "  l_extra  CHAR(1),"
            "PRIMARY KEY (l_orderkey, l_linenumber)"
            ")"
        ),
    },
    {
        "name": "nation",
        "create_cols": (
            "("
            "  n_nationkey    INTEGER PRIMARY KEY,"
            "  n_name         CHAR(25),"
            "  n_regionkey    BIGINT NOT NULL,"
            "  n_comment      VARCHAR(152),"
            "  n_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "region",
        "create_cols": (
            "("
            "  r_regionkey  INTEGER PRIMARY KEY,"
            "  r_name       CHAR(25),"
            "  r_comment    VARCHAR(152),"
            "  r_extra  CHAR(1)"
            ")"
        ),
    },
]


CREATE_TABLE_TEMPLATE = "CREATE TABLE {table_name} {columns}"
DROP_TABLE_TEMPLATE = "DROP TABLE {table_name}"
LOAD_TABLE_TEMPLATE = (
    "COPY {table_name} FROM '{s3_path}' IAM_ROLE '{iam_role}' REGION 'us-east-1'"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tpch-path", type=str, required=True)  # On S3
    parser.add_argument("--drop-tables", action="store_true")
    parser.add_argument("--port", type=int, default=5439)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--iam-role", type=str, required=True)
    args = parser.parse_args()

    rds_user = os.environ["RDS_UID"]
    rds_pass = os.environ["RDS_PWD"]

    conn_str = CONN_STR_TEMPLATE.format(
        "Amazon Redshift (x64)",
        args.host,
        args.port,
        args.user,
        rds_user,
        rds_pass,
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Established connection.", file=sys.stderr)

    if args.drop_tables:
        for table in TABLES:
            print("Dropping", table["name"], file=sys.stderr)
            q = DROP_TABLE_TEMPLATE.format(table_name=table["name"])
            cursor.execute(q)
        cursor.commit()
        return

    for table in TABLES:
        print("Creating", table["name"], file=sys.stderr)
        q = CREATE_TABLE_TEMPLATE.format(
            table_name=table["name"], columns=table["create_cols"]
        )
        cursor.execute(q)
    cursor.commit()

    for table in TABLES:
        print("Loading", table["name"], file=sys.stderr)
        q = LOAD_TABLE_TEMPLATE.format(
            table_name=table["name"],
            s3_path="{}{}/{}.tbl".format(args.tpch_path, table["name"], table["name"]),
            iam_role=args.iam_role,
        )
        cursor.execute(q)
    cursor.commit()


if __name__ == "__main__":
    main()
