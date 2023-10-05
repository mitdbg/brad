import os, json
import time
from pathlib import Path
import yaml
import pandas as pd
import mysql.connector
import platform


from workloads.cross_db_benchmark.benchmark_tools.utils import (
    load_schema_sql,
    load_schema_json,
)


class TiDB:
    def __init__(self):
        self.conn: mysql.connector.MySQLConnection = self.reopen_connection()
        cur = self.conn.cursor()
        cur.execute("SET GLOBAL local_infile = 1;")
        self.conn.commit()

    def reopen_connection(self):
        config_file = "config/tidb.yml"
        with open(config_file, "r") as f:
            config = yaml.load(f, Loader=yaml.Loader)
            self.host = config["host"]
            self.password = config["password"]
            self.user = config["user"]
            self.port = config["port"]
            self.public_key = config["public_key"]
            self.private_key = config["private_key"]
            is_mac = platform.system() == "Darwin"
            if is_mac:
                self.ssl_file = "/etc/ssl/cert.pem"
            else:
                self.ssl_file = "/etc/ssl/certs/ca-certificates.crt"
        conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database="test",
            autocommit=True,
            ssl_ca=self.ssl_file,
            ssl_verify_identity=True,
            allow_local_infile=True,
        )
        cur = conn.cursor()
        cur.execute("SET sql_mode = 'ANSI';")
        conn.commit()
        return conn

    def manually_replicate(self, dataset):
        schema = load_schema_json(dataset)
        for t in schema.tables:
            replica_cmd = f"ALTER TABLE {t} SET TIFLASH REPLICA 1;"
            self.submit_query(replica_cmd, until_success=True)

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

    def submit_query(self, sql: str, until_success: bool = False, error_ok: str = ""):
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
                    self.conn = self.reopen_connection()
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
    tidb = TiDB()
    with tidb.conn.cursor() as cur:
        cur.execute("CREATE TABLE test_table(k INT PRIMARY KEY, v INT);")
        cur.execute("SHOW TABLES;")
        res = cur.fetchall()
        print(f"Results: {res}")
    tidb.load_database("imdb", False)
