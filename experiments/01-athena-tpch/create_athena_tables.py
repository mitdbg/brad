import argparse
import os
import pyodbc
import re
import sys


CONN_STR_TEMPLATE = "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};Schema={};"


TABLES = [
    {
        "name": "part",
        "prefix": "p",
        "create_cols": (
            "("
            "  p_partkey  INT,"
            "  p_name     VARCHAR(55),"
            "  p_mfgr     CHAR(25),"
            "  p_brand    CHAR(10),"
            "  p_type     VARCHAR(25),"
            "  p_size     INT,"
            "  p_container    CHAR(10),"
            "  p_retailprice  DECIMAL,"
            "  p_comment  VARCHAR(23),"
            "  p_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "supplier",
        "prefix": "s",
        "create_cols": (
            "("
            "  s_suppkey   INT,"
            "  s_name	   CHAR(25),"
            "  s_address   VARCHAR(40),"
            "  s_nationkey BIGINT,"
            "  s_phone     CHAR(15),"
            "  s_acctbal   DECIMAL,"
            "  s_comment   VARCHAR(101),"
            "  s_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "partsupp",
        "prefix": "ps",
        "create_cols": (
            "("
            "  ps_partkey     BIGINT,"
            "  ps_suppkey     BIGINT,"
            "  ps_availqty    INT,"
            "  ps_supplycost  DECIMAL,"
            "  ps_comment     VARCHAR(199),"
            "  ps_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "customer",
        "prefix": "c",
        "create_cols": (
            "("
            "  c_custkey    INT,"
            "  c_name       VARCHAR(25),"
            "  c_address    VARCHAR(40),"
            "  c_nationkey  BIGINT,"
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
        "prefix": "o",
        "create_cols": (
            "("
            "  o_orderkey     INT,"
            "  o_custkey      BIGINT,"
            "  o_orderstatus  CHAR(1),"
            "  o_totalprice	  DECIMAL,"
            "  o_orderdate    DATE,"
            "  o_orderpriority	CHAR(15),"
            "  o_clerk        CHAR(15),"
            "  o_shippriority INT,"
            "  o_comment      VARCHAR(79),"
            "  o_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "lineitem",
        "prefix": "l",
        "create_cols": (
            "("
            "  l_orderkey       BIGINT,"
            "  l_partkey        BIGINT,"
            "  l_suppkey        BIGINT,"
            "  l_linenumber     INT,"
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
            "  l_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "nation",
        "prefix": "n",
        "create_cols": (
            "("
            "  n_nationkey    INT,"
            "  n_name         CHAR(25),"
            "  n_regionkey    BIGINT,"
            "  n_comment      VARCHAR(152),"
            "  n_extra  CHAR(1)"
            ")"
        ),
    },
    {
        "name": "region",
        "prefix": "r",
        "create_cols": (
            "("
            "  r_regionkey  INT,"
            "  r_name       CHAR(25),"
            "  r_comment    VARCHAR(152),"
            "  r_extra  CHAR(1)"
            ")"
        ),
    },
]

CREATE_EXTERNAL_TABLE_TEMPLATE = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
    {columns}
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
    STORED AS TEXTFILE
    LOCATION 's3://{s3_path}'
"""

CREATE_ICEBERG_TABLE_TEMPLATE = """
    CREATE TABLE {table_name}
    {columns}
    LOCATION 's3://{s3_path}'
    TBLPROPERTIES ('table_type' = 'ICEBERG')
"""

POPULATE_ICEBERG_TEMPLATE = "INSERT INTO {table_name} SELECT * FROM {source_table}"

CHAR_REPLACE = re.compile("VARCHAR\(\d+\)|CHAR\(\d+\)")

ADD_COLUMN_TEMPLATE = "ALTER TABLE {table_name}_iceberg ADD COLUMNS ({prefix}_{col_name1} {col_type1}, {prefix}_{col_name2} {col_type2})"

SET_COLUMN_VALUE = "UPDATE {table_name}_iceberg SET {prefix}_{col_name1} = {val1}, {prefix}_{col_name2} = {val2}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", type=str, required=True)
    parser.add_argument("--athena-out-path", type=str, default="athena/out/")
    parser.add_argument("--tpch-path", type=str, required=True)  # On S3
    parser.add_argument("--skip-raw", action="store_true")
    parser.add_argument("--skip-iceberg", action="store_true")
    parser.add_argument("--drop-tables", action="store_true")
    parser.add_argument("--add-epoch-columns", action="store_true")
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

    if args.drop_tables:
        for table in TABLES:
            print("> Dropping", table["name"], file=sys.stderr, flush=True)
            cursor.execute("DROP TABLE IF EXISTS {}".format(table["name"]))
            cursor.execute("DROP TABLE IF EXISTS {}_iceberg".format(table["name"]))
        print("> Done", file=sys.stderr, flush=True)
        return

    if args.add_epoch_columns:
        for table in TABLES:
            print("> Adding epoch columns to", table["name"], file=sys.stderr)
            q = ADD_COLUMN_TEMPLATE.format(
                table_name=table["name"],
                prefix=table["prefix"],
                col_name1="epoch_start",
                col_name2="epoch_end",
                col_type1="BIGINT",
                col_type2="BIGINT",
            )
            print("> Adding epoch values to", table["name"], file=sys.stderr)
            cursor.execute(q)
            q = SET_COLUMN_VALUE.format(
                table_name=table["name"],
                prefix=table["prefix"],
                col_name1="epoch_start",
                col_name2="epoch_end",
                val1=1,
                val2=100,
            )
            cursor.execute(q)
        for table in TABLES:
            print("> Running optimize on", table["name"], file=sys.stderr)
            cursor.execute(
                "OPTIMIZE {}_iceberg REWRITE DATA USING BIN_PACK".format(table["name"])
            )
        return

    if not args.skip_raw:
        for table in TABLES:
            print("> Creating", table["name"], file=sys.stderr, flush=True)
            cursor.execute(
                CREATE_EXTERNAL_TABLE_TEMPLATE.format(
                    table_name=table["name"],
                    columns=table["create_cols"],
                    s3_path="{}/{}{}/".format(
                        args.s3_bucket, args.tpch_path, table["name"]
                    ),
                )
            )

    if args.skip_iceberg:
        return

    for table in TABLES:
        print("> Creating iceberg", table["name"], file=sys.stderr, flush=True)
        try:
            iceberg_table = "{}_iceberg".format(table["name"])
            iceberg_cols = re.sub(CHAR_REPLACE, "STRING", table["create_cols"])
            cursor.execute(
                CREATE_ICEBERG_TABLE_TEMPLATE.format(
                    table_name=iceberg_table,
                    columns=iceberg_cols,
                    s3_path="{}/{}{}_iceberg/".format(
                        args.s3_bucket, args.tpch_path, table["name"]
                    ),
                )
            )
            cursor.execute(
                POPULATE_ICEBERG_TEMPLATE.format(
                    table_name=iceberg_table, source_table=table["name"]
                )
            )
        except pyodbc.Error as ex:
            # We expect an error if the table exists. We want to skip the
            # populate step.
            print(str(ex), file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
