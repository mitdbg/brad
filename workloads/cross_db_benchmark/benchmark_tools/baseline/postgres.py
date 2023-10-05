import os
import time
from pathlib import Path
import yaml
import pandas as pd
import psycopg2


from workloads.cross_db_benchmark.benchmark_tools.utils import (
    load_schema_sql,
    load_schema_json,
)


class PostgresCompatible:
    def __init__(self, engine="redshift"):
        self.engine = engine
        self.conn: psycopg2.connection = self.reopen_connection()

    def reopen_connection(self):
        config_file = "config/baseline.yml"
        with open(config_file, "r") as f:
            config = yaml.load(f, Loader=yaml.Loader)
            config = config[self.engine]
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
            p = os.path.join(data_dir, f"{t}.csv")
            table_path = Path(p).resolve()
            baseline_path = os.path.join(data_dir, f"{t}_baseline0.csv")
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
                baseline_path = os.path.join(data_dir, f"{t}_baseline{i}.csv")
                print(f"Writing {t} chunk {i}. ({chunk}/{len(table)}).")
                table.iloc[chunk : chunk + chunksize].to_csv(
                    baseline_path, sep="|", index=False, header=True, na_rep="\\N"
                )
                load_cmd = f"COPY {t} FROM '{baseline_path}' {schema.db_load_kwargs.postgres}"
                print(f"LOAD CMD:\n{load_cmd}")
                self.submit_query(load_cmd, until_success=True)
                print(f"Chunk {i} took {time.perf_counter() - start_t:.2f} secs")
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
    baseline = PostgresCompatible(engine="redshift")
    with baseline.conn.cursor() as cur:
        cur.execute("SELECT 37;")
        res = cur.fetchall()
        print(f"Results: {res}")
