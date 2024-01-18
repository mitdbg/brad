import asyncio
import argparse
import logging

from brad.asset_manager import AssetManager
from brad.blueprint import Blueprint
from brad.blueprint.manager import BlueprintManager
from brad.blueprint.provisioning import Provisioning
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.transition_orchestrator import TransitionOrchestrator
from brad.planner.enumeration.blueprint import EnumeratedBlueprint
from brad.query_rep import QueryRep
from brad.routing.abstract_policy import FullRoutingPolicy
from brad.routing.cached import CachedLocationPolicy
from brad.routing.policy import RoutingPolicy
from brad.routing.tree_based.forest_policy import ForestPolicy
from brad.utils import set_up_logging

logger = logging.getLogger(__name__)


async def run_transition(
    config: ConfigFile,
    blueprint_mgr: BlueprintManager,
    next_blueprint: Blueprint,
) -> None:
    logger.info("Starting the transition...")
    assert next_blueprint is not None
    await blueprint_mgr.start_transition(next_blueprint, new_score=None)
    orchestrator = TransitionOrchestrator(config, blueprint_mgr)
    logger.info("Running the transition...")
    await orchestrator.run_prepare_then_transition()
    logger.info("Running the post-transition clean up...")
    await orchestrator.run_clean_up_after_transition()
    logger.info("Done!")


def main():
    # Push Athena queries onto Redshift.
    # athena_queries = [79, 88]
    athena_queries = []
    aurora_queries = [35, 43, 46]
    redshift_queries = [
        idx
        for idx in list(range(100))
        if idx not in athena_queries and idx not in aurora_queries
    ]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--system-config-file",
        type=str,
        required=True,
        help="Path to BRAD's system configuration file.",
    )
    parser.add_argument(
        "--physical-config-file",
        type=str,
        required=True,
        help="Path to BRAD's physical configuration file.",
    )
    parser.add_argument(
        "--schema-name",
        type=str,
        required=True,
        help="The name of the schema to drop.",
    )
    parser.add_argument("--query-bank-file", type=str)
    parser.add_argument(
        "--athena-queries",
        type=str,
        help="Comma separated list of indices.",
        default=",".join(map(str, athena_queries)),
    )
    parser.add_argument(
        "--aurora-queries",
        type=str,
        help="Comma separated list of indices.",
        default=",".join(map(str, aurora_queries)),
    )
    parser.add_argument(
        "--redshift-queries",
        type=str,
        help="Comma separated list of indices.",
        default=",".join(map(str, redshift_queries)),
    )
    args = parser.parse_args()
    set_up_logging(debug_mode=True)

    # 1. Load the config.
    config = ConfigFile.load_from_new_configs(
        phys_config=args.physical_config_file, system_config=args.system_config_file
    )

    # 2. Load the existing blueprint.
    assets = AssetManager(config)
    blueprint_mgr = BlueprintManager(config, assets, args.schema_name)
    blueprint_mgr.load_sync()
    blueprint = blueprint_mgr.get_blueprint()

    # 3. Load the query bank.
    queries = []
    with open(args.query_bank_file, "r", encoding="UTF-8") as file:
        for line in file:
            clean = line.strip()
            if clean.endswith(";"):
                clean = clean[:-1]
            queries.append(QueryRep(clean))

    # 4. Create the fixed routing policy.
    query_map = {}
    if args.athena_queries is not None:
        for qidx_str in args.athena_queries.split(","):
            qidx = int(qidx_str.strip())
            query_map[queries[qidx]] = Engine.Athena
    if args.redshift_queries is not None:
        for qidx_str in args.redshift_queries.split(","):
            qidx = int(qidx_str.strip())
            query_map[queries[qidx]] = Engine.Redshift
    if args.aurora_queries is not None:
        for qidx_str in args.aurora_queries.split(","):
            qidx = int(qidx_str.strip())
            query_map[queries[qidx]] = Engine.Aurora
    clp = CachedLocationPolicy(query_map)

    # 5. Replace the policy.
    enum_blueprint = EnumeratedBlueprint(blueprint)
    definite_policy = asyncio.run(
        ForestPolicy.from_assets(
            args.schema_name, RoutingPolicy.ForestTableCardinality, assets
        )
    )
    replaced_policy = FullRoutingPolicy(
        indefinite_policies=[clp], definite_policy=definite_policy
    )
    enum_blueprint.set_routing_policy(replaced_policy)

    # Ensure the provisioning is as expected.
    enum_blueprint.set_aurora_provisioning(Provisioning("db.t4g.medium", 2))
    enum_blueprint.set_redshift_provisioning(Provisioning("dc2.large", 4))

    # 6. Adjust the placement.
    new_placement = {}
    aurora_txn = ["theatres", "showings", "ticket_orders", "movie_info", "aka_title"]
    aurora_ana = [
        "aka_name",
        "cast_info",
        "char_name",
        "link_type",
        "movie_link",
        "name",
        "person_info",
        "title",
    ]
    for table in blueprint.tables():
        if table.name in aurora_txn or table.name in aurora_ana:
            new_placement[table.name] = [Engine.Aurora, Engine.Redshift, Engine.Athena]
        else:
            new_placement[table.name] = [Engine.Redshift, Engine.Athena]
    enum_blueprint.set_table_locations(new_placement)

    # 6. Transition to the new blueprint.
    modified_blueprint = enum_blueprint.to_blueprint()
    asyncio.run(run_transition(config, blueprint_mgr, modified_blueprint))


if __name__ == "__main__":
    main()
