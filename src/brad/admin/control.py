import asyncio
import logging
from typing import Awaitable, List

from brad.asset_manager import AssetManager
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.provisioning.directory import Directory
from brad.provisioning.rds import RdsProvisioningManager
from brad.provisioning.rds_status import RdsStatus
from brad.provisioning.redshift import RedshiftProvisioningManager

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "control", help="Used to manually modify BRAD's state for experiments."
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
        help="The schema name to use.",
    )
    parser.add_argument(
        "action",
        type=str,
        help="The action to run {resume, pause}.",
    )
    parser.set_defaults(admin_action=control)


async def control_impl(args) -> None:
    # 1. Load the config, blueprint, and provisioning.
    config = ConfigFile.load_from_new_configs(
        phys_config=args.physical_config_file, system_config=args.system_config_file
    )
    assets = AssetManager(config)

    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    await blueprint_mgr.load()
    blueprint = blueprint_mgr.get_blueprint()

    directory = Directory(config)
    await directory.refresh()

    rds = RdsProvisioningManager(config)

    if args.action == "resume":
        futures: List[Awaitable] = []
        if blueprint.aurora_provisioning().num_nodes() > 0:
            if directory.aurora_writer().status() != RdsStatus.Stopped:
                logger.warning(
                    "Aurora instance %s is not stopped. Not issuing a start command.",
                    config.aurora_cluster_id,
                )
            else:
                futures.append(
                    rds.start_cluster(
                        config.aurora_cluster_id, wait_until_available=True
                    )
                )

        if blueprint.redshift_provisioning().num_nodes() > 0:
            redshift = RedshiftProvisioningManager(config)
            logger.info(
                "Resuming Redshift main cluster: %s", config.redshift_cluster_id
            )
            futures.append(
                redshift.resume_and_fetch_existing_provisioning(
                    config.redshift_cluster_id
                )
            )

        if config.use_preset_redshift_clusters:
            conn_config = config.get_connection_details(Engine.Redshift)
            if "presets" in conn_config:
                for preset in conn_config["presets"]:
                    redshift = RedshiftProvisioningManager(config)
                    logger.info("Resuming Redshift preset: %s", preset["cluster_id"])
                    futures.append(
                        redshift.resume_and_fetch_existing_provisioning(
                            preset["cluster_id"]
                        )
                    )

        # Will block and wait until the engines are ready to accept requests.
        await asyncio.gather(*futures)

    elif args.action == "pause":
        futures = []
        if blueprint.aurora_provisioning().num_nodes() > 0:
            futures.append(rds.pause_cluster(config.aurora_cluster_id))
        if blueprint.redshift_provisioning().num_nodes() > 0:
            redshift = RedshiftProvisioningManager(config)
            futures.append(redshift.pause_cluster(config.redshift_cluster_id))
        if config.use_preset_redshift_clusters:
            conn_config = config.get_connection_details(Engine.Redshift)
            if "presets" in conn_config:
                for preset in conn_config["presets"]:
                    redshift = RedshiftProvisioningManager(config)
                    logger.info("Pausing Redshift preset: %s", preset["cluster_id"])
                    futures.append(redshift.pause_cluster(preset["cluster_id"]))

        # N.B. This will not wait until the shutdown is complete.
        await asyncio.gather(*futures)

    else:
        logger.warning("Unknown action: %s", args.action)

    logger.info("Done.")


# This method is called by `brad.exec.admin.main`.
def control(args):
    asyncio.run(control_impl(args))
