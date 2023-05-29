import collections
import contextlib
from select import select
import socket

import psycopg2
import psycopg2.extensions
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE

# Change these strings so that psycopg2.connect(dsn=dsn_val) works correctly
# for local & remote Postgres.

# JOB/IMDB.
# LOCAL_DSN = "postgres://psycopg:psycopg@localhost/imdb"
LOCAL_DSN = "host=/tmp dbname=imdbload"
REMOTE_DSN = "postgres://psycopg:psycopg@localhost/imdbload"

# TPC-H.
# LOCAL_DSN = "postgres://psycopg:psycopg@localhost/tpch-sf10"
# REMOTE_DSN = "postgres://psycopg:psycopg@localhost/tpch-sf10"

# A simple class holding an execution result.
#   result: a list, outputs from cursor.fetchall().  E.g., the textual outputs
#     from EXPLAIN ANALYZE.
#   has_timeout: bool, set to True iff an execution has run outside of its
#     allocated timeouts; False otherwise (e.g., for non-execution statements
#     such as EXPLAIN).
#   server_ip: str, the private IP address of the Postgres server that
#     generated this Result.
Result = collections.namedtuple("Result", ["result", "has_timeout", "server_ip"],)

# ----------------------------------------
#     Psycopg setup
# ----------------------------------------


# When the driver program (e.g. run.py) is interrupted
# the DB will stop executing in-flight queries peacefully.
# Ref: https://www.psycopg.org/articles/2014/07/20/cancelling-postgresql-statements-python/
def wait_select_inter(conn):
    while 1:
        try:
            state = conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_READ:
                select([conn.fileno()], [], [])
            elif state == POLL_WRITE:
                select([], [conn.fileno()], [])
            else:
                raise conn.OperationalError("bad state from poll: %s" % state)
        except KeyboardInterrupt:
            conn.cancel()
            # the loop will be broken by a server error
            continue


psycopg2.extensions.set_wait_callback(wait_select_inter)


@contextlib.contextmanager
def Cursor(dsn=LOCAL_DSN):
    """Get a cursor to local Postgres database."""
    # TODO: create the cursor once per worker node.
    conn = psycopg2.connect(dsn)
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cursor:
            cursor.execute("load 'pg_hint_plan';")
            yield cursor
    finally:
        conn.close()


# ----------------------------------------
#     Postgres execution
# ----------------------------------------


def _SetGeneticOptimizer(flag, cursor):
    # NOTE: DISCARD would erase settings specified via SET commands.  Make sure
    # no DISCARD ALL is called unexpectedly.
    assert cursor is not None
    assert flag in ["on", "off", "default"], flag
    cursor.execute("set geqo = {};".format(flag))
    assert cursor.statusmessage == "SET"


def ExecuteRemote(sql, verbose=False, geqo_off=False, timeout_ms=None):
    return _ExecuteRemoteImpl.remote(sql, verbose, geqo_off, timeout_ms)


def _ExecuteRemoteImpl(sql, verbose, geqo_off, timeout_ms):
    with Cursor(dsn=REMOTE_DSN) as cursor:
        return Execute(sql, verbose, geqo_off, timeout_ms, cursor)


def Execute(sql, verbose=False, geqo_off=False, timeout_ms=None, cursor=None):
    """Executes a sql statement.

    Returns:
      A dbms_executor.Result.
    """
    if verbose:
        print(sql)

    _SetGeneticOptimizer("off" if geqo_off else "on", cursor)
    if timeout_ms is not None:
        cursor.execute("SET statement_timeout to {}".format(int(timeout_ms)))
    else:
        # Passing None / setting to 0 means disabling timeout.
        cursor.execute("SET statement_timeout to 0")
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        has_timeout = False
    except Exception as e:
        if isinstance(e, psycopg2.errors.QueryCanceled):
            assert "canceling statement due to statement timeout" in str(e).strip(), e
            result = []
            has_timeout = True
        elif isinstance(e, psycopg2.errors.InternalError_):
            print("psycopg2.errors.InternalError_, treating as a" " timeout")
            print(e)
            result = []
            has_timeout = True
        elif isinstance(e, psycopg2.OperationalError):
            if "SSL SYSCALL error: EOF detected" in str(e).strip():
                # This usually indicates an expensive query, putting the server
                # into recovery mode.  'cursor' may get closed too.
                print("Treating as a timeout:", e)
                result = []
                has_timeout = True
            else:
                # E.g., psycopg2.OperationalError: FATAL: the database system
                # is in recovery mode
                raise e
        else:
            raise e
    try:
        _SetGeneticOptimizer("default", cursor)
    except psycopg2.InterfaceError as e:
        # This could happen if the server is in recovery, due to some expensive
        # queries just crashing the server (see the above exceptions).
        assert "cursor already closed" in str(e), e
        pass
    ip = socket.gethostbyname(socket.gethostname())
    return Result(result, has_timeout, ip)


def get_join_node(root):
    if (
        "Join" in root["Node Type"]
        or "join" in root["Node Type"]
        or "Nested Loop" in root["Node Type"]
    ):
        return root
    else:
        if "Plans" in root:
            for node in root["Plans"]:
                return get_join_node(node)


def get_est_card(sql, cursor, return_width=False):
    explain_sql = "EXPLAIN(format json) " + sql
    cursor.execute(explain_sql)
    json_dict = cursor.fetchall()[0][0][0]
    node = get_join_node(json_dict["Plan"])
    num_rows = int(node["Plan Rows"])
    if num_rows < 1:
        num_rows = 1
    if return_width:
        est_width = int(json_dict["Plan"]["Plan Width"])
        return num_rows, est_width
    else:
        return num_rows


def get_true_card(sql, cursor, timeout_ms=30000):
    "Get the true cardinality of a SELECT COUNT(*) query"
    has_timeout = False
    execution_error = False
    if timeout_ms is not None:
        cursor.execute("SET statement_timeout to {}".format(int(timeout_ms)))
    else:
        # Passing None / setting to 0 means disabling timeout.
        cursor.execute("SET statement_timeout to 0")
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        # print(f"execution of {sql} is {result[0][0]}")

    except Exception as e:
        if isinstance(e, psycopg2.errors.QueryCanceled):
            assert "canceling statement due to statement timeout" in str(e).strip(), e
            result = []
            has_timeout = True
        else:
            print(f"fail to execute of {sql}!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(e)
            execution_error = True

    if has_timeout or execution_error:
        print(
            "time out when trying to compute the true card, use the estimated cardinality instead!!!!"
        )
        return get_est_card(sql, cursor), True
    num_rows = int(result[0][0])
    if num_rows < 1:
        num_rows = 1
    return num_rows, False
