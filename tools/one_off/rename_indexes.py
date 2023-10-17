import argparse

from brad.config.file import ConfigFile
from brad.blueprint.user import UserProvidedBlueprint
from brad.config.engine import Engine
from brad.front_end.engine_connections import EngineConnections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str)
    parser.add_argument("--schema-file", type=str)
    args = parser.parse_args()

    bp = UserProvidedBlueprint.load_from_yaml_file(args.schema_file)
    config = ConfigFile.load(args.config_file)
    ecs = EngineConnections.connect_sync(
        config, bp.schema_name, autocommit=False, specific_engines={Engine.Aurora}
    )
    aurora = ecs.get_connection(Engine.Aurora)
    cursor = aurora.cursor_sync()

    # Rename index from "<columns>_index" to "index_<table name>_<columns>"
    for table in bp.tables:
        for indexed_cols in table.secondary_indexed_columns:
            cols = "_".join(map(lambda col: col.name, indexed_cols))
            cursor.execute_sync(
                f"ALTER INDEX IF EXISTS {cols}_index RENAME TO index_{table.name}_{cols}"
            )

    cursor.commit_sync()
    ecs.close_sync()


if __name__ == "__main__":
    main()
