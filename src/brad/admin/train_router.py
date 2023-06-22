import json
from brad.routing.tree_based.trainer import ForestTrainer


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
    parser.set_defaults(admin_action=train_router)


# This method is called by `brad.exec.admin.main`.
def train_router(args):
    trainer = ForestTrainer.load_saved_data(
        schema_file=args.schema_file,
        queries_file=args.data_queries,
        aurora_run_times=args.data_aurora_rt,
        redshift_run_times=args.data_redshift_rt,
        athena_run_times=args.data_athena_rt,
    )
    model, quality = trainer.train()
    print(json.dumps(quality, indent=2))
    model.to_pickle("test.pickle")
