import argparse
import pyodbc
import sys
import os

CONN_STR_TEMPLATE = "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};Schema={};"

TABLES = {
    "customer": ["c_custkey"],
    "orders": ["o_orderkey"],
    "supplier": ["s_suppkey"],
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey"],
}

SPLIT_TABLE_QUERY_TEMPLATE = """
    CREATE TABLE {table_name}_{idx}
    WITH (
        table_type = 'ICEBERG',
        location = 's3://{s3_path}',
        is_external = false
    ) AS
    WITH batched AS (
        SELECT *, NTILE({num_parts}) OVER (ORDER BY {key_col}) AS batch_nbr FROM {table_name}_iceberg
    )
    SELECT * FROM batched WHERE batch_nbr {batch_predicate}
"""

DROP_TABLE_TEMPLATE = "DROP TABLE {table_name}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--drop-split-tables", action="store_true")
    parser.add_argument("--s3-bucket", type=str, required=True)
    parser.add_argument("--athena-out-path", type=str, default="athena/out/")
    parser.add_argument("--tpch-path", type=str, required=True)  # On S3
    args = parser.parse_args()

    aws_key = os.environ["AWS_KEY"]
    aws_secret_key = os.environ["AWS_SECRET_KEY"]

    # We'll split the table into 2 parts, one with 1/10 of the data, the rest
    # with 9/10 of the data.
    num_parts = 10

    conn_str = CONN_STR_TEMPLATE.format(
        "Athena",
        "us-east-1",
        "s3://{}/{}".format(args.s3_bucket, args.athena_out_path),
        aws_key,
        aws_secret_key,
        "iohtap",
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Established connection.", file=sys.stderr)

    if args.drop_split_tables:
        for table_name, keys in TABLES.items():
            q = DROP_TABLE_TEMPLATE.format(table_name="{}_1".format(table_name))
            cursor.execute(q)
            q = DROP_TABLE_TEMPLATE.format(table_name="{}_2".format(table_name))
            cursor.execute(q)
        cursor.commit()
        return

    for table_name, keys in TABLES.items():
        key_col = ", ".join(keys)
        print("Creating {} part 1...".format(table_name), file=sys.stderr)
        q = SPLIT_TABLE_QUERY_TEMPLATE.format(
            table_name=table_name,
            num_parts=num_parts,
            key_col=key_col,
            idx=1,
            batch_predicate="= 1",
            s3_path="{}/{}{}_1/".format(args.s3_bucket, args.tpch_path, table_name),
        )
        print(q)
        cursor.execute(q)
        print("Creating {} part 2...".format(table_name), file=sys.stderr)
        q = SPLIT_TABLE_QUERY_TEMPLATE.format(
            table_name=table_name,
            num_parts=num_parts,
            key_col=key_col,
            idx=2,
            batch_predicate="> 1",
            s3_path="{}/{}{}_2/".format(args.s3_bucket, args.tpch_path, table_name),
        )
        cursor.execute(q)
        cursor.commit()


if __name__ == "__main__":
    main()
