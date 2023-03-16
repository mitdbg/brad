import argparse
import pyodbc
import sys
import os

CONN_STR_TEMPLATE = "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database={};"

TABLES = {
    "customer": ["c_custkey"],
    "orders": ["o_orderkey"],
    "supplier": ["s_suppkey"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey"],
}

SPLIT_TABLE_QUERY_TEMPLATE = """
    CREATE TABLE {table_name}_{part_num}_{num_parts} AS
    WITH batched AS (
        SELECT *, NTILE({num_parts}) OVER (ORDER BY {key_col}) AS batch_nbr FROM {table_name}
    )
    SELECT * FROM batched WHERE batch_nbr = {part_num}
"""


DROP_VIEW_TEMPLATE = "DROP VIEW {view_name}"
DROP_TABLE_TEMPLATE = "DROP TABLE {table_name}"
DEL_COLUMN_TEMPLATE = "ALTER TABLE {table_name} DROP COLUMN {col_name}"
CREATE_VIEW_TEMPLATE = "CREATE VIEW {view_name} AS {view_query}"
VIEW_QUERY_TEMPLATE = "SELECT * FROM {table_name}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5439)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--num-parts", type=int, required=True)
    parser.add_argument("--drop-batch-column", action="store_true")
    parser.add_argument("--create-views", action="store_true")
    parser.add_argument("--drop-split-tables", action="store_true")
    args = parser.parse_args()

    rds_user = os.environ["RDS_UID"]
    rds_pass = os.environ["RDS_PWD"]

    conn_str = CONN_STR_TEMPLATE.format(
        "Amazon Redshift (x64)",
        args.host,
        args.port,
        rds_user,
        rds_pass,
        args.database,
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Established connection.", file=sys.stderr)

    if args.drop_batch_column:
        for table_name, keys in TABLES.items():
            for i in range(args.num_parts):
                q = DEL_COLUMN_TEMPLATE.format(
                    table_name="{}_{}_{}".format(table_name, i + 1, args.num_parts),
                    col_name="batch_nbr",
                )
                cursor.execute(q)
                cursor.commit()
        return

    if args.create_views:
        for table_name, keys in TABLES.items():
            tbls = []
            for i in range(args.num_parts):
                part_table_name = "{}_{}_{}".format(table_name, i + 1, args.num_parts)
                tbls.append(VIEW_QUERY_TEMPLATE.format(table_name=part_table_name))
            q = CREATE_VIEW_TEMPLATE.format(
                view_name="{}_merged".format(table_name),
                view_query=" UNION ALL ".join(tbls),
            )
            cursor.execute(q)
            cursor.commit()
        return

    if args.drop_split_tables:
        for table_name, keys in TABLES.items():
            q = DROP_VIEW_TEMPLATE.format(view_name="{}_merged".format(table_name))
            cursor.execute(q)
            for i in range(args.num_parts):
                q = DROP_TABLE_TEMPLATE.format(
                    table_name="{}_{}_{}".format(table_name, i + 1, args.num_parts)
                )
                cursor.execute(q)
        cursor.commit()
        return

    for table_name, keys in TABLES.items():
        key_col = ", ".join(keys)
        for i in range(args.num_parts):
            print(
                "Creating {} part {} of {}...".format(
                    table_name, i + 1, args.num_parts
                ),
                file=sys.stderr,
            )
            q = SPLIT_TABLE_QUERY_TEMPLATE.format(
                table_name=table_name,
                part_num=i + 1,
                num_parts=args.num_parts,
                key_col=key_col,
            )
            cursor.execute(q)
            cursor.commit()


if __name__ == "__main__":
    main()
