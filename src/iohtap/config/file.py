import yaml

from iohtap.config.dbtype import DBType
from iohtap.config.extraction import ExtractionStrategy


class ConfigFile:
    def __init__(self, path: str):
        with open(path, "r", encoding="UTF-8") as file:
            self._raw = yaml.load(file, Loader=yaml.Loader)

    @property
    def server_interface(self) -> str:
        return self._raw["server_interface"]

    @property
    def server_port(self) -> int:
        return int(self._raw["server_port"])

    @property
    def server_daemon_port(self) -> int:
        return int(self._raw["server_daemon_port"])

    @property
    def athena_s3_data_path(self) -> str:
        return self._raw[DBType.Athena]["s3_data_path"]

    @property
    def extraction_strategy(self) -> ExtractionStrategy:
        return ExtractionStrategy.from_str(self._raw["extraction_strategy"])

    def get_odbc_connection_string(self, db: DBType) -> str:
        if db not in self._raw:
            raise AssertionError("Unhandled database type: " + str(db))

        config = self._raw[db]
        if db is DBType.Athena:
            return "Driver={{{}}};AwsRegion={};S3OutputLocation={};AuthenticationType=IAM Credentials;UID={};PWD={};Schema={};".format(
                config["odbc_driver"],
                config["aws_region"],
                config["s3_output_path"],
                config["access_key"],
                config["access_key_secret"],
                config["database"],
            )

        elif db is DBType.Aurora:
            return "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database={};".format(
                config["odbc_driver"],
                config["host"],
                config["port"],
                config["user"],
                config["password"],
                config["database"],
            )

        elif db is DBType.Redshift:
            return "Driver={{{}}};Server={};Port={};Uid={};Pwd={};Database={};".format(
                config["odbc_driver"],
                config["host"],
                config["port"],
                config["user"],
                config["password"],
                config["database"],
            )
