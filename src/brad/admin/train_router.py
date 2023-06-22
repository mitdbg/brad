import json
import logging
import yaml

from brad.asset_manager import AssetManager
from brad.config.file import ConfigFile
from brad.routing.tree_based.forest_router import ForestRouter
from brad.routing.tree_based.trainer import ForestTrainer

logger = logging.getLogger(__name__)


def register_admin_action(subparser) -> None:
    parser = subparser.add_parser(
        "train_router",
        help="Used to manually train the query router model. "
        "Only use this tool if you know what you are doing!",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="Path to BRAD's configuration file.",
    )
    parser.add_argument(
        "--schema-file",
        type=str,
        required=True,
        help="Path to the schema definition file.",
    )
    parser.add_argument(
        "--data-queries",
        type=str,
        required=True,
        help="Path to the queries to use for training.",
    )
    parser.add_argument(
        "--data-aurora-rt",
        type=str,
        required=True,
        help="Path to the Aurora run times to use for training.",
    )
    parser.add_argument(
        "--data-redshift-rt",
        type=str,
        required=True,
        help="Path to the Redshift run times to use for training.",
    )
    parser.add_argument(
        "--data-athena-rt",
        type=str,
        required=True,
        help="Path to the Athena run times to use for training.",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="If set, the tool will persist the trained model on "
        "S3 for later use by BRAD. Doing so will overwrite any "
        "existing trained routers for the same schema.",
    )
    parser.add_argument(
        "--persist-local",
        action="store_true",
        help="If set, the tool will persist the trained model "
        "locally for debugging purposes.",
    )
    parser.set_defaults(admin_action=train_router)


def extract_schema_name(schema_file: str) -> str:
    with open(schema_file, "r", encoding="UTF-8") as file:
        raw_schema = yaml.load(file, Loader=yaml.Loader)
    return raw_schema["schema_name"]


# This method is called by `brad.exec.admin.main`.
def train_router(args):
    schema_name = extract_schema_name(args.schema_file)
    trainer = ForestTrainer.load_saved_data(
        schema_file=args.schema_file,
        queries_file=args.data_queries,
        aurora_run_times=args.data_aurora_rt,
        redshift_run_times=args.data_redshift_rt,
        athena_run_times=args.data_athena_rt,
    )
    model, quality = trainer.train()
    logger.info("Model quality: %s", json.dumps(quality, indent=2))

    if args.persist:
        config = ConfigFile(args.config_file)
        assets = AssetManager(config)
        ForestRouter.static_persist_sync(model, schema_name, assets)
        logger.info("Model persisted successfully.")

    elif args.persist_local:
        serialized = model.to_pickle()
        file_name = "{}-forest_router.pickle".format(schema_name)
        with open(file_name, "wb") as file:
            file.write(serialized)
        logger.info("Model persisted locally.")

    else:
        logger.info("Not persisting the model.")
