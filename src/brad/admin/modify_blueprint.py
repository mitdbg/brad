import logging

from brad.asset_manager import AssetManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.server.blueprint_manager import BlueprintManager

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
    parser.set_defaults(admin_action=modify_blueprint)


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
