import argparse
import csv
import json
import os.path

from brad.cost_model.preprocessing.feature_statistics import gather_feature_statistics
from brad.cost_model.training.train import train_default, train_readout_hyperparams
from brad.cost_model.dataset.dataset_argment import augment_dataset
from brad.cost_model.training.infer_brad import online_inference_brad
from workloads.cross_db_benchmark.benchmark_tools.autoscale_db import auto_scale
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.run_workload import run_workload
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json, dumper
from workloads.cross_db_benchmark.benchmark_tools.parse_run import (
    parse_queries,
    parse_plans,
)
from workloads.cross_db_benchmark.benchmark_tools.generate_workload import (
    generate_workload,
)
from workloads.cross_db_benchmark.datasets.datasets import database_dict
from workloads.cross_db_benchmark.benchmark_tools.generate_column_stats import (
    generate_stats,
)
from workloads.cross_db_benchmark.benchmark_tools.generate_string_statistics import (
    generate_string_stats,
)


class StoreDictKeyPair(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        my_dict = {}
        for kv in values.split(","):
            k, v = kv.split("=")
            my_dict[k] = v
        setattr(namespace, self.dest, my_dict)


def parse_queries_wrapper(
    database: DatabaseSystem,
    source: str,
    source_aurora: str,
    target: str,
    cap_queries: int,
    db_name: str,
    is_brad: bool,
):
    raw_plans = load_json(source)
    if source_aurora is None or not os.path.exists(source_aurora):
        run_stats_aurora = None
    else:
        run_stats_aurora = load_json(source_aurora)
    parsed_runs, stats = parse_queries(
        database,
        raw_plans,
        run_stats_aurora,
        cap_queries=cap_queries,
        db_name=db_name,
        database_conn_args=args.database_conn_dict,
        use_true_card=args.use_true_card,
        explain_only=args.explain_only,
        timeout_ms=args.query_timeout,
        include_zero_card=args.include_zero_card,
        min_runtime=args.min_query_ms,
        max_runtime=args.max_runtime,
        zero_card_min_runtime=args.min_query_ms * 5,
        target_path=target,
        is_brad=is_brad,
        include_no_joins=args.include_no_joins,
        exclude_runtime_first_run=args.exclude_runtime_first_run,
        only_runtime_first_run=args.only_runtime_first_run,
    )
    with open(target, "w") as outfile:
        json.dump(parsed_runs, outfile, default=dumper)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Scaling a dataset
    parser.add_argument("--scale_dataset", action="store_true")
    parser.add_argument("--scale_factor", default=2, type=int)
    parser.add_argument("--PK_randomness", action="store_true")

    # Generate workload
    parser.add_argument("--generate_workloads", action="store_true")
    parser.add_argument("--no_joins_dist_path", default=None, type=str)
    parser.add_argument("--data_dir", default=None, type=str)
    parser.add_argument("--workload_dir", default=None, type=str)
    parser.add_argument("--force", action="store_true")

    # Run workload commands
    parser.add_argument(
        "--database",
        default=DatabaseSystem.AURORA,
        type=DatabaseSystem,
        choices=list(DatabaseSystem),
    )
    parser.add_argument("--db_name", default="imdb", type=str)
    parser.add_argument(
        "--database_conn",
        dest="database_conn_dict",
        action=StoreDictKeyPair,
        metavar="KEY1=VAL1,KEY2=VAL2...",
    )
    parser.add_argument("--host", default="xxx", type=str)
    parser.add_argument("--port", default="5432", type=str)
    parser.add_argument("--user", default="xxx", type=str)
    parser.add_argument("--sslrootcert", default="SSLCERTIFICATE", type=str)
    parser.add_argument("--password", default="xxx", type=str)
    parser.add_argument("--query_timeout", default=200000, type=int)
    parser.add_argument("--min_query_ms", default=100, type=int)
    parser.add_argument(
        "--database_kwargs",
        dest="database_kwarg_dict",
        action=StoreDictKeyPair,
        metavar="KEY1=VAL1,KEY2=VAL2...",
    )
    parser.add_argument(
        "--run_kwargs",
        dest="run_kwarg_dict",
        action=StoreDictKeyPair,
        metavar="KEY1=VAL1,KEY2=VAL2...",
    )
    parser.add_argument("--target", default="../zero-shot-data/evaluation/imdb_aurora/")
    parser.add_argument("--source", default="")
    parser.add_argument("--repetitions_per_query", default=1, type=int)
    parser.add_argument("--cap_workload", default=100000, type=int)
    parser.add_argument("--with_indexes", action="store_true")
    parser.add_argument("--run_workload", action="store_true")
    parser.add_argument("--re_execute_query_with_no_result", action="store_true")
    # Used to parallelize the data collection.
    parser.add_argument("--run_workload_rank", default=0, type=int)
    parser.add_argument("--run_workload_world_size", default=1, type=int)
    # Needed when collecting data on Athena.
    parser.add_argument("--s3_output_path", type=str)

    # Parse workload command
    parser.add_argument("--parse_plans", action="store_true")
    parser.add_argument("--parse_queries", action="store_true")
    parser.add_argument("--cap_queries", default=50000, type=int)
    parser.add_argument("--include_zero_card", action="store_true")
    parser.add_argument("--use_true_card", action="store_true")
    parser.add_argument("--include_timeout", action="store_true")
    parser.add_argument("--max_runtime", default=200000, type=int)
    parser.add_argument("--explain_only", action="store_true")
    parser.add_argument("--aurora_workload_runs", default=None, nargs="+")
    parser.add_argument("--augment_dataset", action="store_true")
    parser.add_argument("--augment_dataset_dist", type=str)
    parser.add_argument("--is_brad", action="store_true")
    parser.add_argument("--include_no_joins", action="store_true")
    parser.add_argument("--exclude_runtime_first_run", action="store_true")
    parser.add_argument("--only_runtime_first_run", action="store_true")

    # Training cost model command
    parser.add_argument("--workload_runs", default=None, nargs="+")
    parser.add_argument("--test_workload_runs", default=None, nargs="+")
    parser.add_argument(
        "--statistics_file",
        default="../zero-shot-data/runs/parsed_plans/statistics_workload_combined.json",
    )
    parser.add_argument("--raw_dir", default=None)
    parser.add_argument("--loss_class_name", default="QLoss")
    parser.add_argument("--filename_model", default=None)
    parser.add_argument("--device", default="cpu")
    parser.add_argument("--num_workers", type=int, default=1)
    parser.add_argument("--max_epoch_tuples", type=int, default=100000)
    parser.add_argument("--max_no_epochs", type=int, default=None)
    parser.add_argument("--limit_queries", type=int, default=None)
    parser.add_argument("--limit_queries_affected_wl", type=int, default=None)
    parser.add_argument("--limit_num_tables", type=int, default=None)
    parser.add_argument("--limit_runtime", type=int, default=None)
    parser.add_argument("--lower_bound_num_tables", type=int, default=None)
    parser.add_argument("--lower_bound_runtime", type=int, default=None)
    parser.add_argument("--gather_feature_statistics", action="store_true")
    parser.add_argument("--skip_train", action="store_true")
    parser.add_argument("--eval_on_test", action="store_true")
    parser.add_argument("--save_best", action="store_true")
    parser.add_argument("--train_model", action="store_true")
    parser.add_argument("--is_query", action="store_true")
    parser.add_argument("--plan_featurization", default="AuroraEstSystemCardDetail")
    parser.add_argument(
        "--hyperparameter_path",
        default="setup/tuned_hyperparameters/tune_best_config.json",
    )
    parser.add_argument("--seed", type=int, default=0)

    # Brad online inference with cost model
    parser.add_argument("--infer_brad", action="store_true")
    parser.add_argument(
        "--infer_brad_sql_file", type=str, help="file of sql queries to be inferred"
    )
    parser.add_argument(
        "--infer_brad_runtime_file",
        default=None,
        type=str,
        help="file of sql queries to be inferred",
    )
    parser.add_argument(
        "--infer_brad_db_stats_file", type=str, help="file of IMDB database stats"
    )
    parser.add_argument(
        "--infer_brad_model_dir",
        type=str,
        help="directory of trained models for all services",
    )

    args = parser.parse_args()

    if args.database_conn_dict is None:
        args.database_conn_dict = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "sslrootcert": args.sslrootcert,
            "password": args.password,
        }

    if args.database_kwarg_dict is None:
        args.database_kwarg_dict = dict()

    if args.run_kwarg_dict is None:
        args.run_kwarg_dict = dict()

    if args.scale_dataset:
        auto_scale(
            args.source,
            args.target,
            args.db_name,
            args.scale_factor,
            args.PK_randomness,
        )

    if args.generate_workloads:
        workload_defs = {
            # for complex predicates
            "complex_workload_10k_s1": dict(
                num_queries=10000,
                max_no_aggregates=2,
                max_no_group_by=0,
                max_cols_per_agg=1,
                complex_predicates=True,
                max_no_joins=10,
                min_no_joins=4,
                max_no_predicates=6,
                min_no_predicates=2,
                seed=1,
            ),
            "simple_workload_25k_s1": dict(
                num_queries=25000,
                max_no_aggregates=2,
                max_no_group_by=1,
                max_cols_per_agg=1,
                complex_predicates=True,
                max_no_joins=2,
                min_no_joins=0,
                max_no_predicates=4,
                min_no_predicates=1,
                seed=1,
            ),
            # this even simpler
            "simple_workload_25k_s2": dict(
                num_queries=25000,
                max_no_aggregates=2,
                max_no_group_by=1,
                max_cols_per_agg=1,
                complex_predicates=False,
                max_no_joins=2,
                min_no_joins=0,
                max_no_predicates=4,
                min_no_predicates=1,
                seed=2,
            ),
        }

        no_joins_dist = []
        if args.no_joins_dist_path:
            with open(args.no_joins_dist_path) as f:
                no_joins_dist = list(csv.reader(f, delimiter=","))[0]
                no_joins_dist = [float(i) for i in no_joins_dist]

        if args.db_name is not None:
            assert args.db_name in database_dict
            dataset = database_dict[args.db_name]
            data_dir = os.path.join(args.data_dir, dataset.data_folder)
            generate_stats(data_dir, args.db_name, force=args.force)
            generate_string_stats(data_dir, args.db_name, force=args.force)
            for workload_name, workload_args in workload_defs.items():
                workload_path = os.path.join(
                    args.workload_dir, dataset.db_name, f"{workload_name}.sql"
                )
                generate_workload(
                    dataset.source_dataset,
                    workload_path,
                    no_joins_dist=no_joins_dist,
                    **workload_args,
                    force=args.force,
                    full_outer_join=dataset.full_outer_join,
                )

    if args.run_workload:
        run_workload(
            args.source,
            args.database,
            args.db_name,
            args.database_conn_dict,
            args.database_kwarg_dict,
            args.target,
            args.run_kwarg_dict,
            args.repetitions_per_query,
            args.query_timeout,
            with_indexes=args.with_indexes,
            cap_workload=args.cap_workload,
            min_runtime=args.min_query_ms,
            re_execute_query=args.re_execute_query_with_no_result,
            rank=args.run_workload_rank,
            world_size=args.run_workload_world_size,
            s3_output_path=args.s3_output_path,
        )

    if args.parse_plans:
        cap_queries = args.cap_queries
        if cap_queries == "None":
            cap_queries = None
        for workload_file in args.workload_runs:
            raw_plans = load_json(workload_file)
            parsed_runs, stats = parse_plans(
                raw_plans,
                cap_queries=cap_queries,
                include_zero_card=args.include_zero_card,
                max_runtime=args.max_runtime,
            )
            target_path = os.path.join(
                args.target,
                workload_file.split("/")[-1].split(".json")[0] + "_parsed_plan.json",
            )
            with open(target_path, "w") as outfile:
                json.dump(parsed_runs, outfile, default=dumper)

    if args.parse_queries:
        cap_queries = args.cap_queries
        if cap_queries == "None":
            cap_queries = None
        for i, workload_file in enumerate(args.workload_runs):
            if args.aurora_workload_runs is not None and i < len(
                args.aurora_workload_runs
            ):
                aurora_workload_file = args.aurora_workload_runs[i]
            else:
                aurora_workload_file = None
            target = os.path.join(
                args.target,
                workload_file.split("/")[-1].split(".json")[0] + "_parsed_queries.json",
            )
            parse_queries_wrapper(
                args.database,
                workload_file,
                aurora_workload_file,
                target,
                cap_queries,
                args.db_name,
                args.is_brad,
            )

    if args.augment_dataset:
        for i, workload_file in enumerate(args.workload_runs):
            target = os.path.join(
                args.target,
                workload_file.split("/")[-1].split(".json")[0] + "_augmented.json",
            )
            augment_dataset(workload_file, target, args.augment_dataset_dist)

    if args.gather_feature_statistics:
        # gather_feature_statistics
        # workload_runs = []

        # for wl in args.workload_runs:
        #    workload_runs += glob.glob(f'{args.raw_dir}/*/{wl}')
        workload_runs = args.workload_runs
        # for some reason, ignore this file
        broken_files = [
            "../zero-shot-data/runs/parsed_plans/tpc_h/workload_100k_s1_c8220.json"
        ]
        for file in broken_files:
            if file in workload_runs:
                workload_runs.remove(file)

        gather_feature_statistics(workload_runs, args.target)

    if args.train_model:
        if args.hyperparameter_path is None:
            # for testing
            train_default(
                args.workload_runs,
                args.test_workload_runs,
                args.statistics_file,
                args.target,
                args.filename_model,
                plan_featurization=args.plan_featurization,
                device=args.device,
                num_workers=args.num_workers,
                max_epoch_tuples=args.max_epoch_tuples,
                seed=args.seed,
                database=args.database,
                limit_queries=args.limit_queries,
                limit_queries_affected_wl=args.limit_queries_affected_wl,
                max_no_epochs=args.max_no_epochs,
                skip_train=args.skip_train,
                loss_class_name=args.loss_class_name,
                save_best=args.save_best,
                eval_on_test=args.eval_on_test,
            )
        else:
            model = train_readout_hyperparams(
                args.workload_runs,
                args.test_workload_runs,
                args.statistics_file,
                args.target,
                args.filename_model,
                args.hyperparameter_path,
                device=args.device,
                num_workers=args.num_workers,
                max_epoch_tuples=args.max_epoch_tuples,
                seed=args.seed,
                database=args.database,
                limit_queries=args.limit_queries,
                limit_queries_affected_wl=args.limit_queries_affected_wl,
                limit_num_tables=args.limit_num_tables,
                limit_runtime=args.limit_runtime,
                lower_bound_num_tables=args.lower_bound_num_tables,
                lower_bound_runtime=args.lower_bound_runtime,
                max_no_epochs=args.max_no_epochs,
                skip_train=args.skip_train,
                loss_class_name=args.loss_class_name,
                save_best=args.save_best,
                eval_on_test=args.eval_on_test,
            )

    if args.infer_brad:
        with open(args.infer_brad_sql_file, "r") as f:
            workload_sqls = f.readlines()

        database_stats = load_json(args.infer_brad_db_stats_file)

        hyperparameter_paths = {
            "aurora": "src/brad/cost_model/setup/tuned_hyperparameters/aurora_tune_est_best_config.json",
            "redshift": "src/brad/cost_model/setup/tuned_hyperparameters/redshift_tune_est_best_config.json",
            "athena": "src/brad/cost_model/setup/tuned_hyperparameters/athena_tune_est_best_config.json",
        }

        if args.infer_brad_runtime_file is None:
            runtimes = None
        else:
            runtimes = []
            with open(args.infer_brad_runtime_file, "r") as f:
                raw = f.readlines()
            for line in raw:
                db_engine, runtime = tuple(line.split(","))
                runtime = float(runtime.strip())
                runtimes.append((db_engine.split(), runtime))

        pred_result, query_meta_data = online_inference_brad(
            test_workload_sqls=workload_sqls,
            runtimes=runtimes,
            database_stats=database_stats,
            statistics_file=args.statistics_file,
            database_conn_args=args.database_conn_dict,
            filename_model=args.filename_model,
            hyperparameter_paths=hyperparameter_paths,
            model_dir=args.infer_brad_model_dir,
            device="cpu",
            db_name="imdb",
        )
