import yaml
import mysql.connector
import platform


class TiDB:
    def __init__(self):
        config_file = "config/tidb.yml"
        with open(config_file, "r") as f:
            config = yaml.load(f, Loader=yaml.Loader)
            self.host = config["host"]
            self.password = config["password"]
            self.user = config["user"]
            self.port = config["port"]
            is_mac = platform.system() == "Darwin"
            if is_mac:
                self.ssl_file = "/etc/ssl/cert.pem"
            else:
                self.ssl_file = "/etc/ssl/certs/ca-certificates.crt"
        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database="test",
            autocommit=True,
            ssl_ca=self.ssl_file,
            ssl_verify_identity=True,
        )

    def get_connection(self):
        self.conn


if __name__ == "__main__":
    tidb = TiDB()
    with tidb.conn.cursor() as cur:
        cur.execute("CREATE TABLE test_table(k INT PRIMARY KEY, v INT);")
        cur.execute("SHOW TABLES;")
        res = cur.fetchall()
        print(f"Results: {res}")
