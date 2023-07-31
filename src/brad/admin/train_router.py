import asyncio
import json
import logging
import yaml

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.config.file import ConfigFile
from brad.data_stats.estimator import Estimator
from brad.data_stats.postgres_estimator import PostgresEstimator
from brad.routing.policy import RoutingPolicy
from brad.routing.tree_based.forest_router import ForestRouter
from brad.routing.tree_based.trainer import ForestTrainer
from brad.blueprint_manager import BlueprintManager

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
    parser.add_argument(
        "--num-trees",
        type=int,
        default=25,
        help="Used to specify the number of trees in the forest.",
    )
    parser.add_argument(
        "--policy",
        type=str,
        default=RoutingPolicy.ForestTableSelectivity.value,
        help="The type of query router to train. Only the forest-based models "
        "are trainable.",
    )
    parser.set_defaults(admin_action=train_router)


def extract_schema_name(schema_file: str) -> str:
    with open(schema_file, "r", encoding="UTF-8") as file:
        raw_schema = yaml.load(file, Loader=yaml.Loader)
    return raw_schema["schema_name"]


async def set_up_estimator(
    schema_name: str, blueprint: Blueprint, config: ConfigFile
) -> Estimator:
    estimator = await PostgresEstimator.connect(schema_name, config)
    await estimator.analyze(blueprint)
    return estimator


# This method is called by `brad.exec.admin.main`.
def train_router(args):
    schema_name = extract_schema_name(args.schema_file)
    config = ConfigFile(args.config_file)
    policy = RoutingPolicy.from_str(args.policy)

    trainer = ForestTrainer.load_saved_data(
        policy=policy,
        schema_file=args.schema_file,
        queries_file=args.data_queries,
        aurora_run_times=args.data_aurora_rt,
        redshift_run_times=args.data_redshift_rt,
        athena_run_times=args.data_athena_rt,
    )
    if policy == RoutingPolicy.ForestTableSelectivity:
        asset_mgr = AssetManager(config)
        mgr = BlueprintManager(config, asset_mgr, schema_name)
        mgr.load_sync()
        blueprint = mgr.get_blueprint()
        estimator = asyncio.run(set_up_estimator(schema_name, blueprint, config))
    else:
        estimator = None
    trainer.compute_features(estimator)

    logger.info("Routing policy: %s", policy)
    model, quality = trainer.train(num_trees=args.num_trees)
    logger.info("Model quality: %s", json.dumps(quality, indent=2))

    if args.persist_local:
        serialized = model.to_pickle()
        file_name = "{}-{}-router.pickle".format(schema_name, policy.value)
        with open(file_name, "wb") as file:
            file.write(serialized)
        logger.info("Model persisted locally.")

    else:
        while True:
            response = input("Do you want to persist this model? (y/n): ").lower()
            if response == "y":
                assets = AssetManager(config)
                ForestRouter.static_persist_sync(model, schema_name, assets)
                logger.info("Model persisted successfully.")
                break
            elif response == "n":
                logger.info("Not persisting the model.")
                break
            else:
                print("Invalid input. Please enter 'y' or 'n'.")
