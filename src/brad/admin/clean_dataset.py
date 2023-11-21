import asyncio
import logging
from typing import Tuple

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.config.strings import (
    shadow_table_name,
    source_table_name,
    delete_trigger_name,
)
from brad.connection.connection import Connection
from brad.front_end.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


EXPECTED_MAX_IDS = {
    "100gb": {
        "ticket_orders": 993572329,
        "showings": 132480049,
    },
}


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "clean_dataset",
        help="Used to remove rows inserted by the transactions.",
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
        help="The name of the schema.",
    )
    parser.add_argument("--dataset-type", choices=["100gb"])
    parser.add_argument(
        "--engines", nargs="+", default=["aurora", "redshift", "athena"]
    )
    parser.add_argument("--do-clean", action="store_true")
    parser.set_defaults(admin_action=clean_dataset)


async def fetch_dataset_max_ids(connection: Connection) -> Tuple[int, int]:
    cursor = connection.cursor_sync()
    await cursor.execute("SELECT MAX(id) FROM ticket_orders")
    ticket_orders = await cursor.fetchall()
    ticket_orders_id = ticket_orders[0][0]

    await cursor.execute("SELECT MAX(id) FROM showings")
    showings = await cursor.fetchall()
    showings_id = showings[0][0]

    return ticket_orders_id, showings_id


async def clean_dataset_impl(args) -> None:
    # Fetch the expected max and actual max IDs of `ticket_orders` and `showings`.
    # Run the deletion.
    # Clear the shadow tables.
    # Commit.
    config = ConfigFile.load(args.config_file)
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    await blueprint_mgr.load()
    blueprint = blueprint_mgr.get_blueprint()

    engines = {Engine.from_str(engine_str) for engine_str in args.engines}
    if blueprint.aurora_provisioning().num_nodes() == 0:
        engines.remove(Engine.Aurora)
    if blueprint.redshift_provisioning().num_nodes() == 0:
        engines.remove(Engine.Redshift)

    conns = await EngineConnections.connect(
        config,
        blueprint_mgr.get_directory(),
        schema_name=args.schema_name,
        autocommit=False,
        specific_engines=engines,
    )

    # Expected sizes.
    dataset = EXPECTED_MAX_IDS[args.dataset_type]
    for engine in engines:
        cxn = conns.get_connection(engine)
        ticket_orders_id, showings_id = await fetch_dataset_max_ids(cxn)
        logger.info("%s", engine)
        logger.info("Ticket orders ID: %d", ticket_orders_id)
        logger.info("Showings ID: %d", showings_id)
        logger.info(
            "Ticket orders diff (est.): %d", ticket_orders_id - dataset["ticket_orders"]
        )
        logger.info("Showings diff (est.): %d", showings_id - dataset["showings"])

    if not args.do_clean or Engine.Aurora not in engines:
        logger.info(
            "Set `--do-clean` and `--engines aurora` to remove inserted rows from Aurora."
        )
        return
    else:
        logger.info("Removing added rows from Aurora...")

    aurora_cxn = conns.get_connection(Engine.Aurora)
    cursor = await aurora_cxn.cursor()

    logger.info("Disabling the deletion triggers...")
    await cursor.execute(
        "ALTER TABLE {} DISABLE TRIGGER {}".format(
            source_table_name("ticket_orders"), delete_trigger_name("ticket_orders")
        )
    )
    await cursor.execute(
        "ALTER TABLE {} DISABLE TRIGGER {}".format(
            source_table_name("showings"), delete_trigger_name("showings")
        )
    )

    logger.info("Deleting from ticket_orders...")
    await cursor.execute(
        "DELETE FROM ticket_orders WHERE id > {}".format(dataset["ticket_orders"])
    )
    logger.info("Deleting from showings...")
    await cursor.execute(
        "DELETE FROM showings WHERE id > {}".format(dataset["showings"])
    )

    logger.info("Enabling the deletion triggers...")
    await cursor.execute(
        "ALTER TABLE {} ENABLE TRIGGER {}".format(
            source_table_name("ticket_orders"), delete_trigger_name("ticket_orders")
        )
    )
    await cursor.execute(
        "ALTER TABLE {} ENABLE TRIGGER {}".format(
            source_table_name("showings"), delete_trigger_name("showings")
        )
    )

    # Clear the shadow tables too.
    logger.info("Clearing the shadow tables...")
    await cursor.execute("TRUNCATE TABLE {}".format(shadow_table_name("ticket_orders")))
    await cursor.execute("TRUNCATE TABLE {}".format(shadow_table_name("showings")))

    # Commit to finish.
    await cursor.commit()
    logger.info("Done!")

    await conns.close()


# This method is called by `brad.exec.admin.main`.
def clean_dataset(args) -> None:
    asyncio.run(clean_dataset_impl(args))
