import os
import yaml

from iohtap.config.dbtype import DBType


class Config:
    def __init__(self, path: str):
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    def get_odbc_connection_string(self, db: DBType) -> str:
        if db not in self._raw:
            raise AssertionError("Unhandled database type: " + str(db))

        config = self._raw[db]
        if db is DBType.Aurora:
            return "Driver={{{}}}; Server={}; Port={}; Uid={}; Pwd={}".format(
                config["odbc_driver"],
                config["host"],
                config["port"],
                config["user"],
                os.environ[config["pwdvar"]],
            )

        elif db is DBType.Redshift:
            return "Driver={{{}}}; Server={}; Port={}; Uid={}; Pwd={}".format(
                config["odbc_driver"],
                config["host"],
                config["port"],
                config["user"],
                os.environ[config["pwdvar"]],
            )
