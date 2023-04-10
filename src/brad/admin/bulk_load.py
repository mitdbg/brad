import asyncio
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor
from collections import namedtuple
from typing import Any, Coroutine, Iterator, List

from brad.blueprint import Blueprint
from brad.blueprint.sql_gen.table import (
    comma_separated_column_names_and_types,
    comma_separated_column_names,
)
from brad.config.file import ConfigFile
from brad.config.engine import Engine
from brad.config.strings import (
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    AURORA_SEQ_COLUMN,
    source_table_name,
)
from brad.server.blueprint_manager import BlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)

_LoadContext = namedtuple(
    "_LoadContext", ["config", "s3_bucket", "s3_bucket_region", "blueprint"]
)

_AURORA_LOAD_TEMPLATE = """
    SELECT aws_s3.table_import_from_s3(
        '{table_name}',
        '{columns}',
        '{options}',
        aws_commons.create_s3_uri(
            '{s3_bucket}',
            '{s3_path}',
            '{s3_region}'
        )
    );
"""

_REDSHIFT_LOAD_TEMPLATE = """
    COPY {table_name} FROM 's3://{s3_bucket}/{s3_path}'
    IAM_ROLE '{s3_iam_role}'
    {options}
"""

_ATHENA_CREATE_LOAD_TABLE = """
    CREATE EXTERNAL TABLE {load_table_name} ({columns})
    {options1}
    LOCATION 's3://{s3_bucket}/{s3_path}'
    {options2}
"""


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser("bulk_load", help="Bulk load table(s) on BRAD.")
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--manifest-file",
        type=str,
        required=True,
        help="The path to a manifest file, which contains the tables to load.",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="If set, this tool will load the tables one-by-one.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bulk load is normally only allowed when the table(s) are all empty. "
        "Set this flag to override this restriction.",
    )
    parser.set_defaults(admin_action=bulk_load)


async def _ensure_empty(
    manifest, blueprint: Blueprint, engines: EngineConnections
) -> None:
    """
    Verifies that the tables being loaded into are empty.
    """

    try:
        for load_table in manifest["tables"]:
            table_name = load_table["table_name"]
            table = blueprint.get_table(table_name)

            for engine in table.locations:
                conn = engines.get_connection(engine)
                cursor = await conn.cursor()
                await cursor.execute("SELECT COUNT(*) FROM {}".format(table_name))
                row = await cursor.fetchone()
                if row[0] != 0:
                    message = "Table {} on {} is non-empty ({} rows). You can only bulk load non-empty tables.".format(
                        table_name,
                        engine,
                        row[0],
                    )
                    logger.error(message)
                    raise RuntimeError(message)
    finally:
        await engines.get_connection(Engine.Aurora).rollback()
        await engines.get_connection(Engine.Redshift).rollback()


async def _load_aurora(
    ctx: _LoadContext,
    table_name: str,
    table_options,
    aurora_connection,
) -> Engine:
    logger.info("Loading %s on Aurora...", table_name)
    table = ctx.blueprint.get_table(table_name)
    load_query = _AURORA_LOAD_TEMPLATE.format(
        table_name=source_table_name(table),
        columns=comma_separated_column_names(table.columns),
        options=(
            "({})".format(table_options["aurora_options"])
            if "aurora_options" in table_options
            else ""
        ),
        s3_bucket=ctx.s3_bucket,
        s3_region=ctx.s3_bucket_region,
        s3_path=table_options["s3_path"],
    )
    logger.debug("Running on Aurora: %s", load_query)
    cursor = await aurora_connection.cursor()
    await cursor.execute(load_query)

    # Reset the next sequence values for SERIAL/BIGSERIAL types after a bulk
    # load (Aurora does not automatically update it).
    for column in table.columns:
        if column.data_type != "SERIAL" and column.data_type != "BIGSERIAL":
            continue
        q = "SELECT MAX({}) FROM {}".format(column.name, source_table_name(table))
        logger.debug("Running on Aurora: %s", q)
        await cursor.execute(q)
        row = await cursor.fetchone()
        if row is None:
            continue
        max_serial_val = row[0]
        q = "ALTER SEQUENCE {}_{}_seq RESTART WITH {}".format(
            source_table_name(table), column.name, str(max_serial_val + 1)
        )
        logger.debug("Running on Aurora: %s", q)
        await cursor.execute(q)

    logger.info("Done loading %s on Aurora!", table_name)
    return Engine.Aurora


async def _load_redshift(
    ctx: _LoadContext,
    table_name: str,
    table_options,
    redshift_connection,
) -> Engine:
    logger.info("Loading %s on Redshift...", table_name)
    load_query = _REDSHIFT_LOAD_TEMPLATE.format(
        table_name=table_name,
        s3_bucket=ctx.s3_bucket,
        s3_path=table_options["s3_path"],
        options=(
            table_options["redshift_options"]
            if "redshift_options" in table_options
            else ""
        ),
        s3_iam_role=ctx.config.redshift_s3_iam_role,
    )
    logger.debug("Running on Redshift %s", load_query)
    await redshift_connection.execute(load_query)
    logger.info("Done loading %s on Redshift!", table_name)
    return Engine.Redshift


async def _load_athena(
    ctx: _LoadContext,
    table_name: str,
    table_options,
    athena_connection,
) -> Engine:
    logger.info("Loading %s on Athena...", table_name)
    table = ctx.blueprint.get_table(table_name)

    # Strip off the file name from the file path (i.e., we need the prefix)
    # my/table1/table1.csv -> my/table1/
    path_parts = table_options["s3_path"].split("/")
    s3_folder_path = "/".join(path_parts[:-1]) + "/"

    # 1. We need to create a loading table.
    q = _ATHENA_CREATE_LOAD_TABLE.format(
        load_table_name="{}_brad_loading".format(table_name),
        columns=comma_separated_column_names_and_types(table.columns, Engine.Athena),
        s3_bucket=ctx.s3_bucket,
        s3_path=s3_folder_path,
        options1=(
            table_options["athena_options1"]
            if "athena_options1" in table_options
            else ""
        ),
        options2=(
            table_options["athena_options2"]
            if "athena_options2" in table_options
            else ""
        ),
    )
    logger.debug("Running on Athena %s", q)
    await athena_connection.execute(q)

    # 2. Actually run the load.
    q = "INSERT INTO {table_name} SELECT * FROM {table_name}_brad_loading".format(
        table_name=table_name
    )
    logger.debug("Running on Athena %s", q)
    await athena_connection.execute(q)

    # 3. Remove the loading table.
    q = "DROP TABLE {}_brad_loading".format(table_name)
    logger.debug("Running on Athena %s", q)
    await athena_connection.execute(q)

    logger.info("Done loading %s on Athena!", table_name)
    return Engine.Athena


async def _update_sync_progress(
    manifest, blueprint: Blueprint, aurora_connection
) -> None:
    """
    Updates BRAD's sync progress table to ensure that later syncs run correctly.
    """

    cursor = await aurora_connection.cursor()

    q = "SELECT table_name FROM " + AURORA_EXTRACT_PROGRESS_TABLE_NAME
    logger.debug("Running on Aurora %s", q)
    await cursor.execute(q)
    extract_tables = {row[0] for row in await cursor.fetchall()}
    load_tables = {table["table_name"] for table in manifest["tables"]}
    loaded_extract_tables = set.intersection(extract_tables, load_tables)

    for tbl_name in loaded_extract_tables:
        table = blueprint.get_table(tbl_name)
        q = "SELECT MAX({}) FROM {}".format(AURORA_SEQ_COLUMN, source_table_name(table))
        logger.debug("Running on Aurora %s", q)
        await cursor.execute(q)
        row = await cursor.fetchone()
        max_seq = row[0]
        if max_seq is None:
            # The table is still empty.
            continue

        q = "UPDATE {} SET next_extract_seq = {}".format(
            AURORA_EXTRACT_PROGRESS_TABLE_NAME, max_seq + 1
        )
        logger.debug("Running on Aurora %s", q)
        await cursor.execute(q)

    await cursor.commit()


def _try_add_task(
    generator: Iterator[Coroutine[Any, Any, Engine]],
    dest: List[asyncio.Task[Engine] | Coroutine[Any, Any, Engine]],
) -> None:
    try:
        dest.append(next(generator))
    except StopIteration:
        pass


async def bulk_load_impl(args, manifest) -> None:
    config = ConfigFile(args.config_file)
    blueprint_mgr = BlueprintManager(config, manifest["schema_name"])
    await blueprint_mgr.load()
    blueprint = blueprint_mgr.get_blueprint()

    try:
        running: List[asyncio.Task[Engine] | Coroutine[Any, Any, Engine]] = []
        engines = await EngineConnections.connect(config, manifest["schema_name"])
        if not args.force:
            await _ensure_empty(manifest, blueprint, engines)

        ctx = _LoadContext(
            config, manifest["s3_bucket"], manifest["s3_bucket_region"], blueprint
        )

        def load_tasks_for_engine(
            engine: Engine,
        ) -> Iterator[Coroutine[Any, Any, Engine]]:
            for table_options in manifest["tables"]:
                table_name = table_options["table_name"]
                table = blueprint.get_table(table_name)
                if engine == Engine.Aurora and Engine.Aurora in table.locations:
                    yield _load_aurora(
                        ctx,
                        table_name,
                        table_options,
                        engines.get_connection(Engine.Aurora),
                    )
                elif engine == Engine.Redshift and Engine.Redshift in table.locations:
                    yield _load_redshift(
                        ctx,
                        table_name,
                        table_options,
                        engines.get_connection(Engine.Redshift),
                    )
                elif engine == Engine.Athena and Engine.Athena in table.locations:
                    yield _load_athena(
                        ctx,
                        table_name,
                        table_options,
                        engines.get_connection(Engine.Athena),
                    )

        # 2. Execute the load tasks.
        # The intention is to have one in-flight load task for each engine
        # running concurrently.
        aurora_loads = load_tasks_for_engine(Engine.Aurora)
        redshift_loads = load_tasks_for_engine(Engine.Redshift)
        athena_loads = load_tasks_for_engine(Engine.Athena)

        if args.sequential:
            for t in aurora_loads:
                await t
            for t in redshift_loads:
                await t
            for t in athena_loads:
                await t

        else:
            _try_add_task(aurora_loads, running)
            _try_add_task(athena_loads, running)
            _try_add_task(redshift_loads, running)

            while len(running) > 0:
                done, pending = await asyncio.wait(
                    running, return_when=asyncio.FIRST_COMPLETED
                )
                running.clear()
                running.extend(pending)

                for task in done:
                    engine = task.result()
                    if engine == Engine.Aurora:
                        _try_add_task(aurora_loads, running)
                    elif engine == Engine.Redshift:
                        _try_add_task(redshift_loads, running)
                    elif engine == Engine.Athena:
                        _try_add_task(athena_loads, running)

        await engines.get_connection(Engine.Aurora).commit()
        await engines.get_connection(Engine.Redshift).commit()
        # Athena does not support transactions.

        # Update the sync tables.
        await _update_sync_progress(
            manifest, blueprint, engines.get_connection(Engine.Aurora)
        )

    except:
        for to_cancel in running:
            if isinstance(to_cancel, asyncio.Task):
                to_cancel.cancel()
        await asyncio.gather(*running, return_exceptions=True)
        raise

    finally:
        await engines.close()


def bulk_load(args) -> None:
    with open(args.manifest_file, "r", encoding="UTF-8") as file:
        manifest = yaml.load(file, Loader=yaml.Loader)

    executor = ThreadPoolExecutor(max_workers=3)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    asyncio.run(bulk_load_impl(args, manifest))
