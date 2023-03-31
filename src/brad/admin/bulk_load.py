import asyncio
import logging
import yaml
from collections import namedtuple

from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.location import Location
from brad.blueprint.data.table import Table, TableName
from brad.blueprint.sql_gen.table import comma_separated_column_names_and_types
from brad.config.file import ConfigFile
from brad.config.dbtype import DBType
from brad.config.strings import (
    AURORA_EXTRACT_PROGRESS_TABLE_NAME,
    AURORA_SEQ_COLUMN,
    source_table_name,
)
from brad.server.data_blueprint_manager import DataBlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)

_LoadContext = namedtuple("_LoadContext", ["config", "s3_bucket", "s3_bucket_region"])

_AURORA_LOAD_TEMPLATE = """
    SELECT aws_s3.table_import_from_s3(
        '{table_name}',
        '',
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
    IAM_ROLE {s3_iam_role}
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
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema that contains the table(s) to load.",
    )
    parser.add_argument(
        "--manifest-file",
        type=str,
        required=True,
        help="The path to a manifest file, which contains the tables to load.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bulk load is normally only allowed when the table(s) are all empty. "
        "Set this flag to override this restriction.",
    )
    parser.set_defaults(admin_action=bulk_load)


async def _ensure_empty(
    manifest, blueprint: DataBlueprint, engines: EngineConnections
) -> None:
    """
    Verifies that the tables being loaded into are empty.
    """

    for load_table in manifest["tables"]:
        table_name = load_table["table_name"]
        table = blueprint.get_table(table_name)

        for loc in table.locations:
            engine = _location_to_engine(loc)
            conn = engines.get_connection(engine)
            cursor = await conn.cursor()
            await cursor.execute("SELECT COUNT(*) FROM {}".format(table_name))
            row = await cursor.fetchone()
            if row[0] != 0:
                logger.error(
                    "Table %s on %s is non-empty (%d rows). You can only bulk load non-empty tables.",
                    table_name,
                    loc,
                    row[0],
                )
                raise RuntimeError


def _location_to_engine(location: Location) -> DBType:
    if location == Location.Aurora:
        return DBType.Aurora
    elif location == Location.Redshift:
        return DBType.Redshift
    elif location == Location.S3Iceberg:
        return DBType.Athena
    else:
        raise RuntimeError("Unsupported location {}".format(location))


async def _load_aurora(
    ctx: _LoadContext,
    table_name: str,
    table_options,
    aurora_connection,
) -> DBType:
    logger.info("Loading %s on Aurora...", table_name)
    load_query = _AURORA_LOAD_TEMPLATE.format(
        table_name=table_name,
        options=(
            table_options["aurora_options"] if "aurora_options" in table_options else ""
        ),
        s3_bucket=ctx.s3_bucket,
        s3_region=ctx.s3_bucket_region,
        s3_path=table_options["s3_path"],
    )
    logger.debug("Running on Aurora: %s", load_query)
    await aurora_connection.execute(load_query)
    logger.info("Done loading %s on Aurora!", table_name)
    return DBType.Aurora


async def _load_redshift(
    ctx: _LoadContext,
    table_name: str,
    table_options,
    redshift_connection,
) -> DBType:
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
    return DBType.Redshift


async def _load_athena(
    ctx: _LoadContext,
    table_name: str,
    table_options,
    table: Table,
    athena_connection,
) -> DBType:
    logger.info("Loading %s on Athena...", table_name)

    # Strip off the file name from the file path (i.e., we need the prefix)
    # my/table1/table1.csv -> my/table1/
    path_parts = table_options["s3_path"].split("/")
    s3_folder_path = "/".join(path_parts[:-1]) + "/"

    # 1. We need to create a loading table.
    q = _ATHENA_CREATE_LOAD_TABLE.format(
        load_table_name="{}_brad_loading".format(table_name),
        columns=comma_separated_column_names_and_types(table.columns, DBType.Athena),
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
    return DBType.Athena


async def _update_sync_progress(
    ctx: _LoadContext, blueprint: DataBlueprint, aurora_connection
) -> None:
    cursor = await aurora_connection.cursor()

    q = "SELECT table_name FROM " + AURORA_EXTRACT_PROGRESS_TABLE_NAME
    logger.debug("Running on Aurora %s", q)
    await cursor.execute(q)
    extract_tables = await cursor.fetchall()

    for tbl_name in extract_tables:
        table = blueprint.get_table(tbl_name)
        q = "SELECT MAX({}) FROM {}".format(AURORA_SEQ_COLUMN, source_table_name(table))
        logger.debug("Running on Aurora %s", q)
        await cursor.execute(q)
        row = await cursor.fetchone()
        max_seq = row[0]

        q = "UPDATE {} SET next_extract_seq = {}".format(
            AURORA_EXTRACT_PROGRESS_TABLE_NAME, max_seq + 1
        )
        logger.debug("Running on Aurora %s", q)
        await cursor.execute(q)

    await cursor.commit()


async def bulk_load_impl(args, manifest) -> None:
    config = ConfigFile(args.config_file)
    blueprint_mgr = DataBlueprintManager(config, args.schema_name)
    await blueprint_mgr.load()
    blueprint = blueprint_mgr.get_blueprint()

    try:
        engines = await EngineConnections.connect(config, args.schema_name)
        if not args.force:
            await _ensure_empty(manifest, blueprint, engines)

        ctx = _LoadContext(config, manifest["s3_bucket"], manifest["s3_bucket_region"])

        aurora_loads = []
        redshift_loads = []
        athena_loads = []

        # 1. Generate all load tasks.
        for table_options in manifest["tables"]:
            table_name = table_options["table_name"]
            table = blueprint.get_table(TableName(table_name))
            for loc in table.locations:
                if loc == Location.Aurora:
                    aurora_loads.append(
                        _load_aurora(
                            ctx,
                            table_name,
                            table_options,
                            engines.get_connection(DBType.Aurora),
                        )
                    )

                elif loc == Location.Redshift:
                    redshift_loads.append(
                        _load_redshift(
                            ctx,
                            table_name,
                            table_options,
                            engines.get_connection(DBType.Redshift),
                        )
                    )

                elif loc == Location.S3Iceberg:
                    athena_loads.append(
                        _load_athena(
                            ctx,
                            table_name,
                            table_options,
                            table,
                            engines.get_connection(DBType.Athena),
                        )
                    )

                else:
                    raise RuntimeError("Unsupported location {}".format(loc))

        # 2. Execute the load tasks.
        # The intention is to concurrently run at least one load task on each engine.
        aurora_loads.reverse()
        redshift_loads.reverse()
        athena_loads.reverse()

        running_aurora = aurora_loads[-1] if len(aurora_loads) > 0 else None
        running_redshift = redshift_loads[-1] if len(redshift_loads) > 0 else None
        running_athena = athena_loads[-1] if len(athena_loads) > 0 else None

        while len(aurora_loads) > 0 or len(redshift_loads) > 0 or len(athena_loads) > 0:
            running = filter(
                lambda f: f is not None,
                [running_aurora, running_redshift, running_athena],
            )
            done, _ = asyncio.wait(*running, return_when=asyncio.FIRST_COMPLETED)  # type: ignore

            for task in done:
                engine = task.result()
                if engine == DBType.Aurora:
                    aurora_loads.pop()  # type: ignore
                    running_aurora = aurora_loads[-1] if len(aurora_loads) > 0 else None

                elif engine == DBType.Redshift:
                    redshift_loads.pop()  # type: ignore
                    running_redshift = (
                        redshift_loads[-1] if len(redshift_loads) > 0 else None
                    )

                elif engine == DBType.Athena:
                    athena_loads.pop()  # type: ignore
                    running_athena = athena_loads[-1] if len(athena_loads) > 0 else None

        await engines.get_connection(DBType.Aurora).commit()
        await engines.get_connection(DBType.Redshift).commit()
        # Athena does not support transactions.

        # Sanity checks.
        assert len(aurora_loads) == 0
        assert len(redshift_loads) == 0
        assert len(athena_loads) == 0

        # Update the sync tables.
        await _update_sync_progress(
            ctx, blueprint, engines.get_connection(DBType.Aurora)
        )

    finally:
        await engines.close()


def bulk_load(args) -> None:
    with open(args.manifest_file, "r") as file:
        manifest = yaml.load(file, Loader=yaml.Loader)

    asyncio.run(bulk_load_impl(args, manifest))
