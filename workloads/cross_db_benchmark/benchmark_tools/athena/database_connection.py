import time
import json

import multiprocessing
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine
from sqlalchemy import text


def run_one(athena_connection, query, conn, explain_only=False):
    try:
        t = time.perf_counter()
        if explain_only:
            result = athena_connection.execute(text("EXPLAIN (FORMAT JSON) " + query))
        else:
            result = athena_connection.execute(
                text("EXPLAIN ANALYZE (FORMAT JSON) " + query)
            )
        runtime = time.perf_counter() - t
        output = ""
        for row in result:
            output += str(row[0]) + "\n"
        output = json.loads(output)
        output["runtime"] = runtime
    except:
        print("Internal error for query!!!!!!")
        print(query)
        output = None
    conn.send(output)
    conn.close()


class AthenaDatabaseConnection:
    def __init__(self, db_name):
        AWS_ACCESS_KEY = "XX"
        AWS_SECRET_KEY = "XX"
        SCHEMA_NAME = db_name
        S3_STAGING_DIR = "XX"
        AWS_REGION = "us-east-1"
        conn_str = (
            "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.{region_name}.amazonaws.com:"
            "443/{schema_name}?s3_staging_dir={s3_staging_dir}"
        )
        # Create the SQLAlchemy connection. Note that you need to have pyathena installed for this.
        self.conn_str = conn_str.format(
            aws_access_key_id=quote_plus(AWS_ACCESS_KEY),
            aws_secret_access_key=quote_plus(AWS_SECRET_KEY),
            region_name=AWS_REGION,
            schema_name=SCHEMA_NAME,
            s3_staging_dir=quote_plus(S3_STAGING_DIR),
        )
        self.engine = create_engine(self.conn_str)
        self.connection = None

    def run_query_collect_statistics(self, sql, timeout_s=200, explain_only=False):
        self.get_connection()
        analyze_plans = None
        runtime = None
        error = False

        parent_conn, child_conn = multiprocessing.Pipe()
        p1 = multiprocessing.Process(
            target=run_one,
            args=(self.connection, sql, child_conn, explain_only),
            name="athena",
        )
        p1.start()
        p1.join(timeout=timeout_s)
        if p1.is_alive():
            p1.terminate()
            print("Hit the timeout for query!!!!!!")
            print(sql)
            timeout = True
        else:
            timeout = False
            result = parent_conn.recv()
            if result is None:
                error = True
            else:
                runtime = result["runtime"]
                analyze_plans = result

        return dict(
            analyze_plans=analyze_plans, runtime=runtime, timeout=timeout, error=error
        )

    def close_connection(self, close_engine=False):
        try:
            self.connection.close()
            self.connection = None
        except:
            self.connection = None
        if close_engine:
            try:
                self.engine.dispose()
                self.engine = None
            except:
                self.engine = None

    def get_connection(self):
        if self.connection is None:
            try:
                self.connection = self.engine.connect()
            except:
                self.engine = create_engine(self.conn_str)
                self.connection = self.engine.connect()
