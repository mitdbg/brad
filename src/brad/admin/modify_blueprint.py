import argparse
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
from brad.routing.abstract_policy import AbstractRoutingPolicy, FullRoutingPolicy
from brad.routing.always_one import AlwaysOneRouter
from brad.routing.policy import RoutingPolicy
from brad.routing.tree_based.forest_policy import ForestPolicy
from brad.routing.rule_based import RuleBased

logger = logging.getLogger(__name__)


# Parse string-formatted injected table placement.
class ParseTableList(argparse.Action):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.default = {}

    def __call__(self, parser, namespace, s, option_string=None):
        mappings = {}
        for mapping in s.split(";"):
            table, engines_str = mapping.split("=")
            engine_list = engines_str.split(",")
            mappings[table] = [Engine.from_str(e.strip()) for e in engine_list]
        setattr(namespace, self.dest, mappings)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "modify_blueprint",
        help="Make manual edits to a persisted blueprint. "
        "Only use this tool if you know what you are doing!",
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--system-config-file",
        type=str,
        required=True,
        help="Path to BRAD's system configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to modify.",
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
        help="Updates the blueprint's table placement and places tables on all engines. Overrides --place-tables.",
    )
    parser.add_argument(
        "--place-tables",
        action=ParseTableList,
        help="Updates the blueprint's table placement and places the specified tables "
        "on the specified engines. Overridden by --place-tables-everywhere. Format "
        "argument as a string of the form: table1=engine1,engine2;table2=engine3;",
    )
    parser.add_argument(
        "--set-routing-policy",
        choices=[
            "always_redshift",
            "always_aurora",
            "always_athena",
            "df_selectivity",
            "df_cardinality",
            "rule_based",
        ],
        help="Sets the serialized routing policy to a preconfigured default: "
        "{always_redshift, always_aurora, always_athena, df_selectivity, df_cardinality, rule_based}",
    )
    parser.add_argument(
        "--keep-indefinite-policies",
        action="store_true",
        help="If set, will retain the currently-serialized indefinite policies. "
        "This only takes effect when --set-routing-policy is also used.",
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
    parser.add_argument(
        "--abort-transition",
        action="store_true",
        help="Set to abort an in-progress transition. "
        "Only do this if you know what you are doing!",
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
        await blueprint_mgr.start_transition(next_blueprint, new_score=None)
    else:
        logger.info("Continuing the transition...")
    orchestrator = TransitionOrchestrator(config, blueprint_mgr)
    logger.info("Running the transition...")
    await orchestrator.run_prepare_then_transition()
    logger.info("Running the post-transition clean up...")
    await orchestrator.run_clean_up_after_transition()
    logger.info("Done!")


# This method is called by `brad.exec.admin.main`.
def modify_blueprint(args) -> None:
    # 1. Load the config.
    config = ConfigFile.load_from_new_configs(
        phys_config=args.physical_config_file, system_config=args.system_config_file
    )

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
        logger.info("Done!")
        return

    if args.abort_transition:
        asyncio.run(blueprint_mgr.dangerously_abort_transition())
        logger.info("Done!")
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

    # 3. Modify engine provisioning as needed.
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

    # 4. Modify table placement as needed.
    new_placement = blueprint.table_locations().copy()
    for table, engines in args.place_tables.items():
        new_placement[table] = engines
    enum_blueprint.set_table_locations(new_placement)

    if args.place_tables_everywhere:  # Overrides manual placement above.
        new_placement = {}
        for tbl in blueprint.table_locations().keys():
            new_placement[tbl] = Engine.from_bitmap(Engine.bitmap_all())
        enum_blueprint.set_table_locations(new_placement)

    # 5. Modify routing policy as needed.
    if args.set_routing_policy is not None:
        if args.set_routing_policy == "always_redshift":
            definite_policy: AbstractRoutingPolicy = AlwaysOneRouter(Engine.Redshift)
        elif args.set_routing_policy == "always_aurora":
            definite_policy = AlwaysOneRouter(Engine.Aurora)
        elif args.set_routing_policy == "always_athena":
            definite_policy = AlwaysOneRouter(Engine.Athena)
        elif args.set_routing_policy == "df_selectivity":
            definite_policy = asyncio.run(
                ForestPolicy.from_assets(
                    args.schema_name, RoutingPolicy.ForestTableSelectivity, assets
                )
            )
        elif args.set_routing_policy == "rule_based":
            definite_policy = RuleBased()
        elif args.set_routing_policy == "df_cardinality":
            definite_policy = asyncio.run(
                ForestPolicy.from_assets(
                    args.schema_name, RoutingPolicy.ForestTableCardinality, assets
                )
            )
        else:
            raise RuntimeError(
                f"Unknown routing policy preset: {args.set_routing_policy}"
            )

        current_full_policy = enum_blueprint.get_routing_policy()
        if args.keep_indefinite_policies:
            indefinite_policies = current_full_policy.indefinite_policies
        else:
            indefinite_policies = []
        full_policy = FullRoutingPolicy(indefinite_policies, definite_policy)
        enum_blueprint.set_routing_policy(full_policy)

    # 6. Write the changes back.
    modified_blueprint = enum_blueprint.to_blueprint()
    if blueprint == modified_blueprint:
        logger.info("No changes made to the blueprint.")
        return

    if args.force:
        # TODO: If we have an external way to compute the score, we should do it here.
        blueprint_mgr.force_new_blueprint_sync(modified_blueprint, score=None)
    else:
        logger.info("Transitioning to the following blueprint: %s", modified_blueprint)
        asyncio.run(
            run_transition(
                config,
                blueprint_mgr,
                is_continuing=False,
                next_blueprint=modified_blueprint,
            )
        )

    logger.info("Done!")
