import logging

from brad.asset_manager import AssetManager
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.sql_gen.table import (
    generate_create_index_sql,
    generate_drop_index_sql,
)
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.server.blueprint_manager import BlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "modify_blueprint",
        help="Make manual edits to a persisted blueprint. "
        "Only use this tool if you know what you are doing!",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to drop.",
    )
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="If set, just load and print the persisted blueprint.",
    )
    parser.add_argument(
        "--aurora-instance-type",
        type=str,
        help="The Aurora instance type to set.",
    )
    parser.add_argument(
        "--redshift-instance-type",
        type=str,
        help="The Redshift instance type to set.",
    )
    parser.add_argument(
        "--aurora-num-nodes",
        type=int,
        help="The number of Aurora instances to set.",
    )
    parser.add_argument(
        "--redshift-num-nodes",
        type=int,
        help="The number of Redshift instances to set.",
    )
    parser.add_argument(
        "--place-tables-everywhere",
        action="store_true",
        help="Updates the blueprint's table placement and places tables on all engines.",
    )
    parser.add_argument(
        "--add-indexes",
        action="store_true",
        help="Set to create missing indexes where needed.",
    )
    parser.add_argument(
        "--schema-file",
        type=str,
        help="Path to an updated database schema.",
    )
    parser.set_defaults(admin_action=modify_blueprint)


def add_indexes(args, config: ConfigFile, mgr: BlueprintManager) -> None:
    engines = EngineConnections.connect_sync(
        config,
        schema_name=args.schema_name,
        autocommit=False,
        specific_engines={Engine.Aurora},
    )
    try:
        aurora = engines.get_connection(Engine.Aurora)
        cursor = aurora.cursor_sync()

        user = UserProvidedBlueprint.load_from_yaml_file(args.schema_file)
        user.validate()

        tables_with_indexes = {}
        for table in user.tables:
            if len(table.secondary_indexed_columns) == 0:
                continue
            tables_with_indexes[table.name] = table

        current_bp = mgr.get_blueprint()
        for table, locations in current_bp.tables_with_locations():
            if table.name not in tables_with_indexes:
                continue
            if Engine.Aurora not in locations:
                continue
            curr_indexes = set(table.secondary_indexed_columns)
            next_indexes = set(
                tables_with_indexes[table.name].secondary_indexed_columns
            )

            indexes_to_remove = curr_indexes.difference(next_indexes)
            indexes_to_add = next_indexes.difference(curr_indexes)

            sql_to_run = generate_create_index_sql(table, list(indexes_to_add))
            for sql in sql_to_run:
                logger.debug("Running on Aurora: %s", sql)
                cursor.execute_sync(sql)

            sql_to_run = generate_drop_index_sql(table, list(indexes_to_remove))
            for sql in sql_to_run:
                logger.debug("Running on Aurora: %s", sql)
                cursor.execute_sync(sql)

            table.set_secondary_indexed_columns(
                tables_with_indexes[table.name].secondary_indexed_columns
            )

        cursor.commit_sync()
        mgr.persist_sync()
        logger.info("Done!")

    finally:
        engines.close_sync()


# This method is called by `brad.exec.admin.main`.
def modify_blueprint(args):
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Load the existing blueprint.
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(assets, args.schema_name)
    blueprint_mgr.load_sync()
    blueprint = blueprint_mgr.get_blueprint()

    if args.fetch_only:
        print(blueprint)
        return

    if args.add_indexes:
        add_indexes(args, config, blueprint_mgr)
        return

    enum_blueprint = EnumeratedBlueprint(blueprint)

    # 3. Modify parts of the blueprint as needed.
    if args.aurora_instance_type is not None or args.aurora_num_nodes is not None:
        aurora_prov = blueprint.aurora_provisioning()
        aurora_prov = aurora_prov.mutable_clone()
        if args.aurora_instance_type is not None:
            aurora_prov.set_instance_type(args.aurora_instance_type)
        if args.aurora_num_nodes is not None:
            aurora_prov.set_num_nodes(args.aurora_num_nodes)
        enum_blueprint.set_aurora_provisioning(aurora_prov)

    if args.redshift_instance_type is not None or args.redshift_num_nodes is not None:
        redshift_prov = blueprint.redshift_provisioning()
        redshift_prov = redshift_prov.mutable_clone()
        if args.redshift_instance_type is not None:
            redshift_prov.set_instance_type(args.redshift_instance_type)
        if args.redshift_num_nodes is not None:
            redshift_prov.set_num_nodes(args.redshift_num_nodes)
        enum_blueprint.set_redshift_provisioning(redshift_prov)

    if args.place_tables_everywhere:
        new_placement = {}
        for tbl in blueprint.table_locations().keys():
            new_placement[tbl] = Engine.from_bitmap(Engine.bitmap_all())
        enum_blueprint.set_table_locations(new_placement)

    # 3. Write the changes back.
    modified_blueprint = enum_blueprint.to_blueprint()
    blueprint_mgr.set_blueprint(modified_blueprint)
    blueprint_mgr.persist_sync()

    logger.info("Done!")
