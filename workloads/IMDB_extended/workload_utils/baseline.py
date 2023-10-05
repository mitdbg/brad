import yaml
import mysql.connector
import psycopg2
import os, json
import time
from pathlib import Path
import pandas as pd
import platform
from types import SimpleNamespace
import boto3


def load_schema_json(dataset):
    schema_path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "schema.json"
    )
    assert os.path.exists(schema_path), f"Could not find schema.json ({schema_path})"
    return json.load(open(schema_path, mode='r', encoding='utf-8'), object_hook=lambda d: SimpleNamespace(**d))


def load_schema_sql(dataset, sql_filename):
    sql_path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "schema_sql", sql_filename
    )
    assert os.path.exists(sql_path), f"Could not find schema.sql ({sql_path})"
    with open(sql_path, "r") as file:
        data = file.read().replace("\n", "")
    return data







def make_tidb_conn():
    config_file = "config/baseline.yml"
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.Loader)
        config = config["tidb"]
        host = config["host"]
        password = config["password"]
        user = config["user"]
        port = config["port"]
        is_mac = platform.system() == "Darwin"
        if is_mac:
            ssl_file = "/etc/ssl/cert.pem"
        else:
            ssl_file = "/etc/ssl/certs/ca-certificates.crt"

        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database="test",
            ssl_ca=ssl_file,
            ssl_verify_identity=True,
            allow_local_infile=True,
        )
        cur = conn.cursor()
        cur.execute("SET sql_mode = 'ANSI';")
        conn.commit()
        cur.close()
        return conn


def make_postgres_compatible_conn(engine="redshift"):
    config_file = "config/baseline.yml"
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.Loader)
        config = config[engine]
        host = config["host"]
        password = config["password"]
        user = config["user"]
        port = config["port"]
        database = config["database"]
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    return conn


class TiDBLoader:
    def __init__(self):
        self.conn: mysql.connector.MySQLConnection = make_tidb_conn()
        cur = self.conn.cursor()
        cur.execute("SET GLOBAL local_infile = 1;")
        self.conn.commit()            

    def load_database(self, dataset, data_dir, force=False, load_from: str = ""):
        # First, check existence.
        print(f"Checking existence. Force={force}")
        exists = self.check_exists(dataset)
        if exists and not force and load_from == "":
            return
        # Create tables.
        print("Creating tables.")
        if load_from == "":
            schema_sql = load_schema_sql(dataset, "mysql.sql")
            self.submit_query(schema_sql)
        # Load data.
        print("Loading data.")
        schema = load_schema_json(dataset)
        start_loading = load_from == ""
        for t in schema.tables:
            if t == load_from:
                start_loading = True
            if not start_loading:
                continue
            start_t = time.perf_counter()
            p = os.path.join(data_dir, f"{t}.csv")
            table_path = Path(p).resolve()
            tidb_path = os.path.join(data_dir, f"{t}_tidb0.csv")
            table = pd.read_csv(
                table_path,
                delimiter=",",
                quotechar='"',
                escapechar="\\",
                na_values="",
                keep_default_na=False,
                header=0,
                low_memory=False,
            )
            # Need to load chunk by chunk to avoid networking errors.
            chunksize = 1_000_000
            print(f"Loading {t}. {len(table)} rows.")
            for i, chunk in enumerate(range(0, len(table), chunksize)):
                # Also need to rewrite nulls.
                tidb_path = os.path.join(data_dir, f"{t}_tidb{i}.csv")
                print(f"Writing {t} chunk {i}. ({chunk}/{len(table)}).")
                table.iloc[chunk : chunk + chunksize].to_csv(
                    tidb_path, sep="|", index=False, header=True, na_rep="\\N"
                )
                load_cmd = f"LOAD DATA LOCAL INFILE '{tidb_path}' INTO TABLE {t} {schema.db_load_kwargs.mysql}"
                print(f"LOAD CMD:\n{load_cmd}")
                self.submit_query(load_cmd, until_success=True)
                print(f"Chunk {i} took {time.perf_counter() - start_t:.2f} secs")
            print(f"Loaded {t} in {time.perf_counter() - start_t:.2f} secs")
            print(f"Replicating {t} for HTAP")
            replica_cmd = f"ALTER TABLE {t} SET TIFLASH REPLICA 1"
            self.submit_query(replica_cmd, until_success=True)

        # print("Creating Indexes")
        # indexes_sql = load_schema_sql(dataset, "indexes.sql")
        # self.submit_query(indexes_sql)

    # Check if all the tables in the given dataset already exist.
    def check_exists(self, dataset):
        schema = load_schema_json(dataset)
        for t in schema.tables:
            q = f"""
                SELECT 
                    TABLE_SCHEMA,TABLE_NAME, TABLE_TYPE
                FROM 
                    information_schema.TABLES 
                WHERE 
                    TABLE_SCHEMA LIKE 'test' AND
                    TABLE_TYPE LIKE 'BASE TABLE' AND
                    TABLE_NAME = '{t}';
            """
            res = self.run_query_with_results(q)
            print(f"Tables: {res}")
            if len(res) == 0:
                return False
        return True

    def get_connection(self):
        self.conn

    def submit_query(self, sql: str, until_success: bool = False):
        while True:
            try:
                cur = self.conn.cursor()
                # cur.execute(sql)
                commands = sql.split(";")

                for command in commands:
                    command = command.strip()
                    if len(command) > 0:
                        print(f"Running Query: {command}")
                        cur.execute(command)
                self.conn.commit()
                return
            except mysql.connector.Error as err:
                err_str = f"{err}"

                if not until_success:
                    raise err
                if "Lost connection" in err_str:
                    self.conn = make_tidb_conn()
                    continue
                print(f"Not a retryable error: {err}")
                raise err

    def run_query_with_results(self, sql: str):
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        self.conn.commit()
        return res


class PostgresCompatibleLoader:
    def __init__(self, engine="redshift"):
        self.engine = engine
        self.conn: psycopg2.connection = make_postgres_compatible_conn(engine=engine)
        config_file = "config/baseline.yml"
        with open(config_file, "r") as f:
            config = yaml.load(f, Loader=yaml.Loader)
            self.s3_bucket = config["s3_bucket"]
            self.bucket_region = config["bucket_region"]
            config = config[engine]
            if engine == "redshift":
                self.iam_role = config["iam"]
            else:
                self.access_key = config["access_key"]
                self.secret_key = config["secret_key"]
        if engine == "aurora":
            cur = self.conn.cursor()
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;")
            self.conn.commit()


    def manually_copy_s3_data(self, dataset):
        schema = load_schema_json(dataset)
        s3 = boto3.resource('s3')
        # Hacky: relies on specifc ordering
        reached_title = False
        for t in schema.tables:
            if t == "title":
                reached_title = True
            if reached_title:
                source_dir = "imdb_20G"
            else:
                source_dir = "imdb_extended_20g"
            source_key = f"{source_dir}/{t}/{t}.csv"
            target_key = f"imdb_extended/{t}/{t}.csv"
            copy_source = {
                'Bucket': 'geoffxy-research',
                'Key': source_key
            }
            print(f"Copying {t}")
            start_t = time.perf_counter()
            s3.meta.client.copy(copy_source, self.s3_bucket, target_key)
            print(f"Copied {t} in {time.perf_counter() - start_t:.2f} secs")
            

    def make_load_cmd(self, t, load_args) -> str:
        if self.engine == "redshift":
            path = f"s3://{self.s3_bucket}/imdb_extended/{t}/{t}.csv"
            load_args = load_args.redshift
            load_cmd = f"COPY {t} FROM '{path}' {load_args} iam_role '{self.iam_role}'"
        else:
            path = f"imdb_extended/{t}/{t}.csv"
            load_args = load_args.aurora
            load_cmd = f"""
            SELECT aws_s3.table_import_from_s3(
                '{t}',
                '',
                '({load_args})',
                aws_commons.create_s3_uri(
                    '{self.s3_bucket}',
                    '{path}',
                    '{self.bucket_region}'
                ),
                aws_commons.create_aws_credentials('{self.access_key}', '{self.secret_key}', '')
            );
            """
        return load_cmd        

    def load_database(self, dataset, data_dir, force=False, load_from: str = ""):
        # First, check existence.
        print(f"Checking existence. Force={force}")
        exists = self.check_exists(dataset)
        if exists and not force and load_from == "":
            return
        # Create tables.
        print("Creating tables.")
        if load_from == "":
            schema_sql = load_schema_sql(dataset, "postgres.sql")
            self.submit_query(schema_sql)
        # Load data.
        print("Loading data.")
        schema = load_schema_json(dataset)
        start_loading = load_from == ""
        for t in schema.tables:
            if t == load_from:
                start_loading = True
            if not start_loading:
                continue
            start_t = time.perf_counter()
            print(f"Loading {t}.")
            load_cmd = self.make_load_cmd(t, schema.db_load_kwargs)
            print(f"LOAD CMD:\n{load_cmd}")
            self.submit_query(load_cmd, until_success=True)
            print(f"Loaded {t} in {time.perf_counter() - start_t:.2f} secs")

    # Check if all the tables in the given dataset already exist.
    def check_exists(self, dataset):
        schema = load_schema_json(dataset)
        for t in schema.tables:
            q = f"""
                SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename='{t}'
            """
            res = self.run_query_with_results(q)
            print(f"Tables: {res}")
            if len(res) == 0:
                return False
        return True

    def get_connection(self):
        self.conn

    def submit_query(self, sql: str, until_success: bool = False, error_ok: str = ""):
        while True:
            try:
                cur = self.conn.cursor()
                # cur.execute(sql)
                commands = sql.split(";")

                for command in commands:
                    command = command.strip()
                    if len(command) > 0:
                        if self.engine == "redshift" and command.upper().startswith("CREATE INDEX"):
                            print(f"Skipping index for redshift: {command}!")
                            continue
                        if self.engine == "redshift" and command.upper().startswith("CREATE"):
                            command = command.replace("SERIAL", "INTEGER")
                            command = command.replace("serial", "integer")
                            command = command.replace("TEXT", "VARCHAR(65535)")
                            command = command.replace("text", "varchar(65535)")
                        print(f"Running Query: {command}")
                        cur.execute(command)
                self.conn.commit()
                return
            except psycopg2.Error as err:
                err_str = f"{err}"
                # TODO: make psycopg2 specific.
                if not until_success:
                    raise err
                if "Lost connection" in err_str:
                    self.conn = make_postgres_compatible_conn(engine=self.engine)
                    continue
                print(f"Not a retryable error: {err}")
                raise err

    def run_query_with_results(self, sql: str):
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        self.conn.commit()
        return res


if __name__ == "__main__":
    baseline = PostgresCompatibleLoader(engine="aurora")
    with baseline.conn.cursor() as cur:
        s3_bucket = baseline.s3_bucket
        region = baseline.bucket_region
        t = "theaters"
        path = f"s3://{s3_bucket}/imdb_extended/{t}/{t}.csv"
        cur.execute(f"SELECT aws_commons.create_s3_uri('{s3_bucket}', '{path}', '{region}');")
        res = cur.fetchall()
        print(f"Results: {res}")
    # import sys
    # if len(sys.argv) > 1 and sys.argv[1] == "copy":
    #     baseline.manually_copy_s3_data("imdb_extended")