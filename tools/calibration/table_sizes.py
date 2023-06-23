import argparse
import logging
import json
from typing import Dict

from brad.asset_manager import AssetManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.server.blueprint_manager import BlueprintManager
from brad.server.engine_connections import EngineConnections
from brad.utils.table_sizer import TableSizer
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        "Run this after bootstrapping a schema to measure table sizing "
        "constants used by the blueprint planner."
    )
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    set_up_logging(debug_mode=args.debug)

    config = ConfigFile(args.config_file)
    assets = AssetManager(config)
    mgr = BlueprintManager(assets, args.schema_name)
    mgr.load_sync()

    bp = mgr.get_blueprint()
    logger.info("Using blueprint: %s", bp)

    engine_set = {Engine.Aurora, Engine.Athena, Engine.Redshift}
    if bp.aurora_provisioning().num_nodes() == 0:
        engine_set.remove(Engine.Aurora)
    if bp.redshift_provisioning().num_nodes() == 0:
        engine_set.remove(Engine.Redshift)

    engines = EngineConnections.connect_sync(
        config, args.schema_name, autocommit=True, specific_engines=engine_set
    )
    table_sizer = TableSizer(engines, config)

    overall: Dict[str, Dict[Engine, float]] = {}

    for table, locations in bp.tables_with_locations():
        per_table: Dict[Engine, float] = {}
        for loc in locations:
            num_rows = table_sizer.table_size_rows(table.name, loc)
            table_bytes = table_sizer.table_size_bytes(table.name, loc)
            b_per_row = table_bytes / num_rows
            per_table[loc] = b_per_row
            logger.info(
                "%s on %s: %d rows, %d B total", table.name, loc, num_rows, table_bytes
            )
        overall[table.name] = per_table

    if args.debug:
        logger.debug("Recorded stats: %s", json.dumps(overall, indent=2))

    print("table_bytes_per_row:", flush=True)
    print(f"  {args.schema_name}:")
    for table, size_info in overall.items():
        print(f"    {table}:", flush=True)
        for engine, b_per_row in size_info.items():
            print(f"      {engine.value}: {b_per_row}", flush=True)


if __name__ == "__main__":
    main()
