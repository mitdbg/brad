import asyncio
import argparse

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.connection.connection import Connection
from brad.front_end.engine_connections import EngineConnections


_REDSHIFT_LOAD_TEMPLATE = """
    COPY telemetry (ip, timestamp, movie_id, event_id) FROM 's3://{s3_bucket}/{s3_path}'
    IAM_ROLE '{s3_iam_role}'
    CSV IGNOREHEADER 1 delimiter '|' BLANKSASNULL
"""

_ATHENA_CREATE_LOAD_TABLE = """
    CREATE EXTERNAL TABLE telemetry_base (ip, timestamp, movie_id, event_id)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' NULL DEFINED AS '' STORED AS TEXTFILE
    LOCATION 's3://{s3_bucket}/{s3_path}'
    TBLPROPERTIES ('skip.header.line.count' = '1')
"""


async def load_redshift(args, config: ConfigFile, connection: Connection):
    cursor = connection.cursor_sync()

    times = args.load_times
    print(f"[Redshift] Loading the base data {times} times")
    for idx in range(times):
        print(f"[Redshift] Time {idx + 1} of {times}")
        await cursor.execute(
            _REDSHIFT_LOAD_TEMPLATE.format(
                s3_bucket=args.data_s3_bucket,
                s3_path=args.data_s3_path,
                s3_iam_role=config.redshift_s3_iam_role,
            )
        )
        cursor.commit_sync()


async def load_athena(args, connection: Connection):
    cursor = connection.cursor_sync()

    print("[Athena] Registering the base table.")
    await cursor.execute(
        _ATHENA_CREATE_LOAD_TABLE.format(
            s3_bucket=args.data_s3_bucket,
            s3_path=args.data_s3_path,
        )
    )

    times = args.load_times
    print(f"[Athena] Loading the base data {times} times")
    for idx in range(times):
        print(f"[Athena] Time {idx + 1} of {times}")
        await cursor.execute("INSERT INTO telemetry SELECT 0, * FROM telemetry_base")

    print("[Athena] Dropping the base table.")
    await cursor.execute("DROP TABLE telemetry_base")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--schema-name", type=str, required=True)
    parser.add_argument("--data-s3-bucket", type=str, required=True)
    parser.add_argument("--data-s3-path", type=str, required=True)
    parser.add_argument("--load-times", type=int, default=1)
    parser.add_argument("--engines", type=str, nargs="+")
    args = parser.parse_args()

    engines = {Engine.from_str(engine) for engine in args.engines}

    config = ConfigFile.load(args.config_file)
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    await blueprint_mgr.load()

    cxns = await EngineConnections.connect(
        config,
        blueprint_mgr.get_directory(),
        args.schema_name,
        autocommit=True,
        specific_engines=engines,
    )

    futures = []
    if Engine.Athena in engines:
        futures.append(load_athena(args, cxns.get_connection(Engine.Athena)))
    if Engine.Redshift in engines:
        futures.append(
            load_redshift(args, config, cxns.get_connection(Engine.Redshift))
        )
    await asyncio.gather(*futures)
    await cxns.close()


if __name__ == "__main__":
    asyncio.run(main())
