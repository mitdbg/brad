import argparse

from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.connection.factory import ConnectionFactory
from brad.connection.odbc_connection import OdbcConnection


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, required=True)
    parser.add_argument("--port", type=str, required=True)
    parser.add_argument("--physical-config-file", type=str, required=True)
    args = parser.parse_args()

    config = ConfigFile.load_from_physical_config(args.physical_config_file)
    cstr = ConnectionFactory._pg_aurora_odbc_connection_string(
        args.address,
        args.port,
        config.get_connection_details(Engine.Aurora),
        schema_name=None,
    )
    cxn: Connection = OdbcConnection.connect_sync(cstr, autocommit=True, timeout_s=30)
    cursor = cxn.cursor_sync()
    cursor.execute_sync("SELECT 1")
    print(cursor.fetchall_sync())

    cxn.close_sync()


if __name__ == "__main__":
    main()
