import asyncio
import logging
from typing import Optional

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.user import UserProvidedBlueprint
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.sql_gen.table import (
    generate_create_index_sql,
    generate_drop_index_sql,
)
from brad.blueprint.state import TransitionState
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.transition_orchestrator import TransitionOrchestrator
from brad.front_end.engine_connections import EngineConnections
from brad.planner.enumeration.blueprint import EnumeratedBlueprint

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
    parser.add_argument(
        "--force",
        action="store_true",
        help="Set to force persist the blueprint and treat it as stable. "
        "If not set, this tool will prepare to transition to the modified blueprint.",
    )
    parser.add_argument(
        "--continue-transition",
        action="store_true",
        help="Set to resume a transition that was already started but not "
        "necessarily completed.",
    )
    parser.set_defaults(admin_action=modify_blueprint)


def add_indexes(args, config: ConfigFile, mgr: BlueprintManager) -> None:
    engines = EngineConnections.connect_sync(
        config,
        mgr.get_directory(),
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

            if len(indexes_to_remove) == 0 and len(indexes_to_add) == 0:
                # Create indexes just to be safe.
                sql_to_run = generate_create_index_sql(table, list(next_indexes))
                for sql in sql_to_run:
                    logger.debug("Running on Aurora: %s", sql)
                    cursor.execute_sync(sql)
                continue

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
        logger.info("Done!")

    finally:
        engines.close_sync()


async def run_transition(
    config: ConfigFile,
    blueprint_mgr: BlueprintManager,
    is_continuing: bool,
    next_blueprint: Optional[Blueprint],
) -> None:
    if not is_continuing:
        logger.info("Starting the transition...")
        assert next_blueprint is not None
        await blueprint_mgr.start_transition(next_blueprint)
    else:
        logger.info("Continuing the transition...")
    orchestrator = TransitionOrchestrator(config, blueprint_mgr)
    logger.info("Running the transition...")
    await orchestrator.run_prepare_then_transition()
    logger.info("Running the post-transition clean up...")
    await orchestrator.run_clean_up_after_transition()
    logger.info("Done!")


# This method is called by `brad.exec.admin.main`.
def modify_blueprint(args):
    # 1. Load the config.
    config = ConfigFile(args.config_file)

    # 2. Load the existing blueprint.
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    blueprint_mgr.load_sync()
    blueprint = blueprint_mgr.get_blueprint()

    if args.fetch_only:
        print(blueprint)
        return

    if args.add_indexes:
        add_indexes(args, config, blueprint_mgr)
        return

    if args.continue_transition:
        asyncio.run(
            run_transition(
                config, blueprint_mgr, is_continuing=True, next_blueprint=None
            )
        )
        return

    tm = blueprint_mgr.get_transition_metadata()
    if tm.state != TransitionState.Stable:
        logger.warning(
            "A transition is already in progress (current state: %s)",
            str(tm.state),
        )
        if not args.force:
            logger.fatal("Not proceeding because --force is not set.")
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
    if args.force:
        blueprint_mgr.force_new_blueprint_sync(modified_blueprint)
    else:
        asyncio.run(
            run_transition(
                config,
                blueprint_mgr,
                is_continuing=False,
                next_blueprint=modified_blueprint,
            )
        )

    logger.info("Done!")
