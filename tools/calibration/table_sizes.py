import boto3
import asyncio
import argparse
import logging
import json
from typing import Dict

from brad.asset_manager import AssetManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.data_sync.execution.context import ExecutionContext
from brad.data_sync.operators.unload_to_s3 import UnloadToS3
from brad.server.blueprint_manager import BlueprintManager
from brad.server.engine_connections import EngineConnections
from brad.utils.table_sizer import TableSizer
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


def s3_object_size_bytes(client, bucket: str, key: str) -> int:
    response = client.head_object(Bucket=bucket, Key=key)
    return response["ContentLength"]


def delete_s3_object(client, bucket: str, key: str) -> None:
    client.delete_object(Bucket=bucket, Key=key)


async def main_impl(args):
    config = ConfigFile(args.config_file)
    assets = AssetManager(config)
    mgr = BlueprintManager(assets, args.schema_name)
    await mgr.load()

    bp = mgr.get_blueprint()
    logger.info("Using blueprint: %s", bp)

    engines_sync = EngineConnections.connect_sync(
        config, args.schema_name, autocommit=True, specific_engines={Engine.Aurora}
    )
    engines = await EngineConnections.connect(
        config, args.schema_name, autocommit=True, specific_engines={Engine.Aurora}
    )

    boto_client = boto3.client(
        "s3",
        aws_access_key_id=config.aws_access_key,
        aws_secret_access_key=config.aws_access_key_secret,
    )
    table_sizer = TableSizer(engines_sync, config)
    ctx = ExecutionContext(
        engines.get_connection(Engine.Aurora), None, None, bp, config
    )

    # We need a rough measure of how much data we will export and import per
    # table (in bytes) when transferring tables. We will unload data from Aurora
    # to estimate the transfer size per row.

    bytes_per_row: Dict[str, float] = {}

    for table, locations in bp.tables_with_locations():
        if Engine.Aurora not in locations:
            logger.warning(
                "Skipping %s because it is not present on Aurora.", table.name
            )
            continue

        extract_file = f"{table.name}_test.tbl"
        full_extract_path = f"{config.s3_extract_path}{extract_file}"

        num_rows = table_sizer.table_size_rows(table.name, Engine.Aurora)
        op = UnloadToS3(table.name, extract_file, Engine.Aurora)
        await op.execute(ctx)

        table_bytes = s3_object_size_bytes(
            boto_client, config.s3_extract_bucket, full_extract_path
        )

        b_per_row = table_bytes / num_rows
        logger.info(
            "%s: %d rows, extracted %d B total", table.name, num_rows, table_bytes
        )
        bytes_per_row[table.name] = b_per_row

        delete_s3_object(boto_client, config.s3_extract_bucket, full_extract_path)

    if args.debug:
        logger.debug("Recorded stats: %s", json.dumps(bytes_per_row, indent=2))

    print("table_extract_bytes_per_row:", flush=True)
    print(f"  {args.schema_name}:")
    for table, bpr in bytes_per_row.items():
        print(f"    {table}: {bpr}", flush=True)

    await engines.close()
    engines_sync.close_sync()


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

    asyncio.run(main_impl(args))


if __name__ == "__main__":
    main()