import asyncio
import logging

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.manager import BlueprintManager
from brad.config.file import ConfigFile
from brad.daemon.transition_orchestrator import TransitionOrchestrator

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "restore_blueprint",
        help="Used to restore BRAD to a previous blueprint version. Note that "
        "this creates a new blueprint version that is a copy of the previous "
        "blueprint.",
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
        help="The schema name to use.",
    )
    parser.add_argument(
        "--blueprint-version",
        type=int,
        help="The blueprint version to use.",
    )
    parser.add_argument(
        "--transition",
        action="store_true",
        help="If set, transition to the historical blueprint.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Set to force persist the blueprint and treat it as stable.",
    )
    parser.set_defaults(admin_action=restore_blueprint)


async def run_transition(
    config: ConfigFile,
    blueprint_mgr: BlueprintManager,
    next_blueprint: Blueprint,
) -> None:
    await blueprint_mgr.start_transition(next_blueprint, new_score=None)
    orchestrator = TransitionOrchestrator(config, blueprint_mgr)
    logger.info("Running the transition...")
    await orchestrator.run_prepare_then_transition()
    logger.info("Running the post-transition clean up...")
    await orchestrator.run_clean_up_after_transition()
    logger.info("Done!")


async def restore_blueprint_impl(args) -> None:
    config = ConfigFile.load(args.config_file)
    assets = AssetManager(config)

    version = args.blueprint_version
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    historical_blueprint, _ = await blueprint_mgr.fetch_historical_version(version)

    logger.info("Historical blueprint version %d", version)
    logger.info("%s", historical_blueprint)

    if not args.transition:
        logger.info("Re-run with --transition to transition to this blueprint.")
        return

    await blueprint_mgr.load()
    if not args.force:
        logger.info("Forcing a change to version %d", version)
        await run_transition(config, blueprint_mgr, historical_blueprint)
    else:
        logger.info("Transitioning to version %d", version)
        blueprint_mgr.force_new_blueprint_sync(historical_blueprint, score=None)


# This method is called by `brad.exec.admin.main`.
def restore_blueprint(args):
    asyncio.run(restore_blueprint_impl(args))
