import argparse
import pyodbc
import sys
import os

CONN_STR_TEMPLATE = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database={};"

TABLES = [
    {
        "name": "part",
        "prefix": "p",
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
            "  p_epoch_start  BIGINT,"
            "  p_epoch_end  BIGINT"
            ")"
        ),
    },
    {
        "name": "supplier",
        "prefix": "s",
        "create_cols": (
            "("
            "  s_suppkey   INTEGER PRIMARY KEY,"
            "  s_name			CHAR(25),"
            "  s_address   VARCHAR(40),"
            "  s_nationkey BIGINT NOT NULL,"
            "  s_phone     CHAR(15),"
            "  s_acctbal   DECIMAL,"
            "  s_comment   VARCHAR(101),"
            "  s_epoch_start  BIGINT,"
            "  s_epoch_end  BIGINT"
            ")"
        ),
    },
    {
        "name": "partsupp",
        "prefix": "ps",
        "create_cols": (
            "("
            "  ps_partkey     BIGINT NOT NULL,"
            "  ps_suppkey     BIGINT NOT NULL,"
            "  ps_availqty    INTEGER,"
            "  ps_supplycost  DECIMAL,"
            "  ps_comment     VARCHAR(199),"
            "  ps_epoch_start  BIGINT,"
            "  ps_epoch_end  BIGINT,"
            "  PRIMARY KEY (ps_partkey, ps_suppkey)"
            ")"
        ),
    },
    {
        "name": "customer",
        "prefix": "c",
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
            "  c_epoch_start  BIGINT,"
            "  c_epoch_end  BIGINT"
            ")"
        ),
    },
    {
        "name": "orders",
        "prefix": "o",
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
            "  o_epoch_start  BIGINT,"
            "  o_epoch_end  BIGINT"
            ")"
        ),
    },
    {
        "name": "lineitem",
        "prefix": "l",
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
            "  l_epoch_start  BIGINT,"
            "  l_epoch_end  BIGINT,"
            "PRIMARY KEY (l_orderkey, l_linenumber)"
            ")"
        ),
    },
    {
        "name": "nation",
        "prefix": "n",
        "create_cols": (
            "("
            "  n_nationkey    INTEGER PRIMARY KEY,"
            "  n_name         CHAR(25),"
            "  n_regionkey    BIGINT NOT NULL,"
            "  n_comment      VARCHAR(152),"
            "  n_epoch_start  BIGINT,"
            "  n_epoch_end  BIGINT"
            ")"
        ),
    },
    {
        "name": "region",
        "prefix": "r",
        "create_cols": (
            "("
            "  r_regionkey  INTEGER PRIMARY KEY,"
            "  r_name       CHAR(25),"
            "  r_comment    VARCHAR(152),"
            "  r_epoch_start  BIGINT,"
            "  r_epoch_end  BIGINT"
            ")"
        ),
    },
]


CREATE_INDEX_TEMPLATE = (
    "CREATE INDEX {index_name} ON {table_name} USING btree ({index_col})"
)
DROP_INDEX_TEMPLATE = "DROP INDEX {index_name}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop-indexes", action="store_true")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--database", type=str, required=True)
    args = parser.parse_args()

    aur_user = os.environ["AUR_UID"]
    aur_pass = os.environ["AUR_PWD"]

    conn_str = CONN_STR_TEMPLATE.format(
        "Postgres",
        args.host,
        args.port,
        aur_user,
        aur_pass,
        args.database,
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Established connection.", file=sys.stderr)

    if args.drop_indexes:
        for table in TABLES:
            print("Dropping indexes on", table["name"], file=sys.stderr)
            cursor.execute("DROP INDEX {}_es".format(table["name"]))
            cursor.execute("DROP INDEX {}_ee".format(table["name"]))
        cursor.commit()
        return

    for table in TABLES:
        print("Creating indexes on", table["name"], file=sys.stderr)
        q = CREATE_INDEX_TEMPLATE.format(
            index_name="{}_es".format(table["name"]),
            table_name=table["name"],
            index_col="{}_epoch_start".format(table["prefix"]),
        )
        cursor.execute(q)
        q = CREATE_INDEX_TEMPLATE.format(
            index_name="{}_ee".format(table["name"]),
            table_name=table["name"],
            index_col="{}_epoch_end".format(table["prefix"]),
        )
        cursor.execute(q)
    cursor.commit()


if __name__ == "__main__":
    main()
