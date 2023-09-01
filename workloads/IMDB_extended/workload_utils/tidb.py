import yaml
import platform
import mysql.connector


def make_tidb_odbc():
    config_file = "config/tidb.yml"
    with open(config_file, "r") as f:
        config = yaml.load(f, Loader=yaml.Loader)
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
