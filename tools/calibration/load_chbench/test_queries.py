import argparse
import asyncio
from brad.config.file import ConfigFile
from brad.connection.factory import ConnectionFactory
from brad.config.engine import Engine
from brad.provisioning.directory import Directory


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--physical-config-file", type=str, required=True)
    parser.add_argument("--query-file", type=str, required=True)
    args = parser.parse_args()

    with open(args.query_file, "r", encoding="UTF-8") as file:
        queries = [line.strip() for line in file]

    config = ConfigFile.load_from_physical_config(args.physical_config_file)
    directory = Directory(config)
    asyncio.run(directory.refresh())
    connection = ConnectionFactory.connect_to_sync(
        Engine.Redshift, args.schema_name, config, directory, autocommit=True
    )

    cursor = connection.cursor_sync()
    num_succeeded = 0
    for idx, q in enumerate(queries):
        try:
            print("Running query", idx, "of", len(queries) - 1)
            cursor.execute_sync(q)
            num_succeeded += 1
        except Exception as ex:
            print("Query", idx, "failed with error", str(ex))

    if num_succeeded == len(queries):
        print("All succeeded.")
    else:
        print((len(queries) - num_succeeded), "failed.")


if __name__ == "__main__":
    main()
