import sys
import logging

from brad.utils import set_up_logging
import brad.admin.bootstrap_schema as bootstrap_schema
import brad.admin.drop_schema as drop_schema
import brad.admin.bulk_load as bulk_load
import brad.admin.run_planner as run_planner
import brad.admin.modify_blueprint as modify_blueprint
import brad.admin.train_router as train_router
import brad.admin.workload_logs as workload_logs
import brad.admin.run_on as run_on
import brad.admin.control as control
import brad.admin.restore_blueprint as restore_blueprint
import brad.admin.replay_planner as replay_planner
import brad.admin.clean_dataset as clean_dataset
import brad.admin.alter_schema as alter_schema
import brad.admin.table_adjustments as table_adjustments

logger = logging.getLogger(__name__)


def register_command(subparsers) -> None:
    parser = subparsers.add_parser(
        "admin",
        help="Used to run administrative tasks on the underlying engines.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Set to enable debug logging.",
    )
    admin_subparsers = parser.add_subparsers(title="Administrative Actions")
    bootstrap_schema.register_admin_action(admin_subparsers)
    drop_schema.register_admin_action(admin_subparsers)
    bulk_load.register_admin_action(admin_subparsers)
    run_planner.register_admin_action(admin_subparsers)
    modify_blueprint.register_admin_action(admin_subparsers)
    train_router.register_admin_action(admin_subparsers)
    workload_logs.register_admin_action(admin_subparsers)
    run_on.register_admin_action(admin_subparsers)
    control.register_admin_action(admin_subparsers)
    restore_blueprint.register_admin_action(admin_subparsers)
    replay_planner.register_admin_action(admin_subparsers)
    clean_dataset.register_admin_action(admin_subparsers)
    alter_schema.register_admin_action(admin_subparsers)
    table_adjustments.register_admin_action(admin_subparsers)
    parser.set_defaults(func=main)


def main(args):
    set_up_logging(debug_mode=args.debug)

    if "admin_action" not in args:
        logger.error("Please specify an admin action.")
        sys.exit(1)

    args.admin_action(args)
