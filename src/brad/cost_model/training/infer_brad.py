import numpy as np
import torch
import collections
import torch.optim as opt
from tqdm import tqdm

from brad.cost_model.dataset.dataset_creation import create_dataloader_for_brad
from brad.cost_model.training.checkpoint import load_checkpoint
from brad.cost_model.training.utils import batch_to
from brad.cost_model.encoder.specific_models.model import zero_shot_models
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json
from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.load_database import create_db_conn
from workloads.cross_db_benchmark.benchmark_tools.parse_run import parse_query_generic


def rename_database_stats(database_stats):
    for col_stats in database_stats.column_stats:
        if not col_stats.tablename.endswith(
            "_brad_source"
        ) and not col_stats.tablename.endswith("_brad_shadow"):
            col_stats.tablename = col_stats.tablename + "_brad_source"
    for tab_stats in database_stats.table_stats:
        if not tab_stats.relname.endswith(
            "_brad_source"
        ) and not tab_stats.relname.endswith("_brad_shadow"):
            tab_stats.relname = tab_stats.relname + "_brad_source"


def load_model_one(
    database,
    model_dir,
    filename_model,
    feature_statistics,
    optimizer_class_name="Adam",
    optimizer_kwargs=None,
    final_mlp_kwargs=None,
    node_type_kwargs=None,
    model_kwargs=None,
    tree_layer_name="GATConv",
    tree_layer_kwargs=None,
    hidden_dim=32,
    output_dim=1,
    device="cpu",
    plan_featurization_name=None,
):
    if model_kwargs is None:
        model_kwargs = dict()

    model = zero_shot_models[database](
        device=device,
        hidden_dim=hidden_dim,
        final_mlp_kwargs=final_mlp_kwargs,
        node_type_kwargs=node_type_kwargs,
        output_dim=output_dim,
        feature_statistics=feature_statistics,
        tree_layer_name=tree_layer_name,
        tree_layer_kwargs=tree_layer_kwargs,
        query_featurization_name=plan_featurization_name,
        label_norm=None,
        **model_kwargs,
    )
    # move to gpu
    model = model.to(model.device)
    optimizer = opt.__dict__[optimizer_class_name](
        model.parameters(), **optimizer_kwargs
    )
    (
        csv_stats,
        epochs_wo_improvement,
        epoch,
        model,
        optimizer,
        metrics,
        finished,
    ) = load_checkpoint(
        model,
        model_dir,
        filename_model,
        optimizer=optimizer,
        metrics=None,
        filetype=".pt",
    )
    return model


def load_models(
    statistics_file,
    filename_model,
    hyperparameter_paths,
    model_dir,
    device="cpu",
    loss_class_name="QLoss",
):
    # load the trained cost model for aurora, redshift, and athena
    feature_statistics = load_json(statistics_file, namespace=False)
    model_aurora = None
    model_redshift = None
    model_athena = None
    for database in hyperparameter_paths:
        hyperparameter_path = hyperparameter_paths[database]

        hyperparams = load_json(hyperparameter_path, namespace=False)

        p_dropout = hyperparams.pop("p_dropout")
        # general fc out
        fc_out_kwargs = dict(
            p_dropout=p_dropout,
            activation_class_name="LeakyReLU",
            activation_class_kwargs={},
            norm_class_name="Identity",
            norm_class_kwargs={},
            residual=hyperparams.pop("residual"),
            dropout=hyperparams.pop("dropout"),
            activation=True,
            inplace=True,
        )
        final_mlp_kwargs = dict(
            width_factor=hyperparams.pop("final_width_factor"),
            n_layers=hyperparams.pop("final_layers"),
            loss_class_name=loss_class_name,
            loss_class_kwargs=dict(),
        )
        tree_layer_kwargs = dict(
            width_factor=hyperparams.pop("tree_layer_width_factor"),
            n_layers=hyperparams.pop("message_passing_layers"),
        )
        node_type_kwargs = dict(
            width_factor=hyperparams.pop("node_type_width_factor"),
            n_layers=hyperparams.pop("node_layers"),
            one_hot_embeddings=True,
            max_emb_dim=hyperparams.pop("max_emb_dim"),
            drop_whole_embeddings=False,
        )
        final_mlp_kwargs.update(**fc_out_kwargs)
        tree_layer_kwargs.update(**fc_out_kwargs)
        node_type_kwargs.update(**fc_out_kwargs)

        train_kwargs = dict(
            optimizer_class_name="AdamW",
            optimizer_kwargs=dict(
                lr=hyperparams.pop("lr"),
            ),
            final_mlp_kwargs=final_mlp_kwargs,
            node_type_kwargs=node_type_kwargs,
            tree_layer_kwargs=tree_layer_kwargs,
            tree_layer_name=hyperparams.pop("tree_layer_name"),
            plan_featurization_name=hyperparams.pop("plan_featurization_name"),
            hidden_dim=hyperparams.pop("hidden_dim"),
            output_dim=1,
            device=device,
        )
        if database == "aurora":
            model_aurora = load_model_one(
                DatabaseSystem.AURORA,
                model_dir,
                filename_model + "_aurora",
                feature_statistics,
                **train_kwargs,
            )
        elif database == "redshift":
            model_redshift = load_model_one(
                DatabaseSystem.REDSHIFT,
                model_dir,
                filename_model + "_redshift",
                feature_statistics,
                **train_kwargs,
            )
        elif database == "athena":
            model_athena = load_model_one(
                DatabaseSystem.ATHENA,
                model_dir,
                filename_model + "_athena",
                feature_statistics,
                **train_kwargs,
            )
    return model_aurora, model_redshift, model_athena, feature_statistics


def parse_query_for_brad(
    test_workload_sqls,
    runtimes,
    database_stats,
    database_conn_args,
    engines,
    database_kwarg_dict=None,
    db_name="imdb",
):
    # assume that runtimes are list of tuple [(engine, runtime)] for each query in test_workload_sqls or None
    if database_kwarg_dict is None:
        database_kwarg_dict = dict()
    db_conn = create_db_conn(
        DatabaseSystem.AURORA, db_name, database_conn_args, database_kwarg_dict
    )
    column_id_mapping = dict()
    table_id_mapping = dict()

    partial_column_name_mapping = collections.defaultdict(set)

    # enrich column stats with table sizes
    table_sizes = dict()
    for table_stat in database_stats.table_stats:
        table_sizes[table_stat.relname] = table_stat.reltuples

    for i, column_stat in enumerate(database_stats.column_stats):
        table = column_stat.tablename
        column = column_stat.attname
        column_stat.table_size = table_sizes[table]
        column_id_mapping[(table, column)] = i
        partial_column_name_mapping[column].add(table)

    # similar for table statistics
    for i, table_stat in enumerate(database_stats.table_stats):
        table = table_stat.relname
        table_id_mapping[table] = i

    parsed_queries = []
    parsed_query_sql = []
    skipped_query_idx = []
    parsed_query_idx = []

    aurora_query_idx = []
    redshift_query_idx = []
    athena_query_idx = []

    for i, sql in enumerate(tqdm(test_workload_sqls)):
        verbose_plan = None
        try:
            verbose_plan = db_conn.get_result(f"EXPLAIN VERBOSE {sql}")
        except:
            print(f"WARNING skipping query {i}: {sql} due to error in Aurora EXPLAIN")
            skipped_query_idx.append(i)
            parsed_queries.append(None)
            parsed_query_sql.append(None)
            continue
        parsed_query_idx.append(i)
        parsed_query = parse_query_generic(
            verbose_plan,
            sql,
            db_conn,
            database_stats,
            column_id_mapping,
            table_id_mapping,
            partial_column_name_mapping,
        )
        if parsed_query is None:
            print(f"WARNING skipping query {i}: {sql} due to an error in Parsing")
            skipped_query_idx.append(i)
            parsed_queries.append(None)
            parsed_query_sql.append(None)
            continue

        parsed_query.database_id = 0  # Current we only have one database
        if runtimes is not None:
            engine, runtime = runtimes[i]
            if engine == "aurora":
                redshift_query_idx.append(i)
                athena_query_idx.append(i)
            elif engine == "redshift":
                aurora_query_idx.append(i)
                athena_query_idx.append(i)
            elif engine == "athena":
                redshift_query_idx.append(i)
                aurora_query_idx.append(i)

            parsed_query.plan_runtime = runtime
            parsed_query.runtime = runtimes[i]
        else:
            aurora_query_idx.append(i)
            redshift_query_idx.append(i)
            athena_query_idx.append(i)
            parsed_query.plan_runtime = (
                1.0  # random number so that we don't need to rewrite data loader
            )

        parsed_queries.append(parsed_query)
        parsed_query_sql.append(sql)

    parsed_runs = dict(
        parsed_queries=parsed_queries,
        sql_queries=parsed_query_sql,
        database_stats=database_stats,
        run_kwargs=None,
    )
    query_meta_data = dict(
        aurora_query_idx=np.asarray(aurora_query_idx),
        redshift_query_idx=np.asarray(redshift_query_idx),
        athena_query_idx=np.asarray(athena_query_idx),
        skipped_query_idx=np.asarray(skipped_query_idx),
        parsed_query_idx=np.asarray(parsed_query_idx),
    )
    return parsed_runs, query_meta_data


def infer_one_engine(data_loader, model):
    with torch.autograd.no_grad():
        preds = []
        for batch_idx, batch in enumerate(tqdm(data_loader)):
            input_model, _, _, _ = batch_to(batch, model.device, model.label_norm)
            output = model(input_model)

            curr_pred = output.cpu().numpy()
            if model.label_norm is not None:
                curr_pred = model.label_norm.inverse_transform(curr_pred)
            preds.append(curr_pred.reshape(-1))

        preds = np.concatenate(preds, axis=0)
        return preds


def online_inference_brad(
    test_workload_sqls,
    runtimes,
    database_stats,
    database_conn_args,
    statistics_file,
    filename_model,
    hyperparameter_paths,
    model_dir,
    device="cpu",
    loss_class_name="QLoss",
    db_name="imdb",
    engines=None,
):
    # read the logged queries and infer the query runtime
    if runtimes is not None:
        assert len(test_workload_sqls) == len(runtimes)
    if engines is None:
        engines = ["aurora", "redshift", "athena"]
    rename_database_stats(database_stats)
    # load the models
    model_aurora, model_redshift, model_athena, feature_statistics = load_models(
        statistics_file,
        filename_model,
        hyperparameter_paths,
        model_dir,
        device,
        loss_class_name,
    )

    # parsed the queries
    parsed_runs, query_meta_data = parse_query_for_brad(
        test_workload_sqls,
        runtimes,
        database_stats,
        database_conn_args,
        engines,
        db_name=db_name,
    )

    database_statistics = dict()  # current only support one database for brad
    database_statistics[0] = database_stats
    database_statistics[0].run_kwars = None

    aurora_loader = create_dataloader_for_brad(
        DatabaseSystem.AURORA,
        query_meta_data["aurora_query_idx"],
        parsed_runs,
        database_statistics,
        feature_statistics,
        "AuroraEstSystemCardDetail",
    )
    aurora_pred = infer_one_engine(aurora_loader, model_aurora)

    redshift_loader = create_dataloader_for_brad(
        DatabaseSystem.REDSHIFT,
        query_meta_data["redshift_query_idx"],
        parsed_runs,
        database_statistics,
        feature_statistics,
        "RedshiftEstSystemCardDetail",
    )
    redshift_pred = infer_one_engine(redshift_loader, model_redshift)

    athena_loader = create_dataloader_for_brad(
        DatabaseSystem.ATHENA,
        query_meta_data["athena_query_idx"],
        parsed_runs,
        database_statistics,
        feature_statistics,
        "AthenaEstSystemCardDetail",
    )
    athena_pred = infer_one_engine(athena_loader, model_athena)

    # if a query a not parsable, we use np.infty as an indicator
    pred_result = np.zeros((len(test_workload_sqls), 3)) + np.infty
    pred_result[query_meta_data["aurora_query_idx"], 0] = aurora_pred
    pred_result[query_meta_data["redshift_query_idx"], 1] = redshift_pred
    pred_result[query_meta_data["athena_query_idx"], 2] = athena_pred

    if runtimes is not None:
        for i in range(len(test_workload_sqls)):
            curr_engine, curr_runtime = runtimes[i]
            curr_engine_idx = engines.index(curr_engine)
            pred_result[i, curr_engine_idx] = curr_runtime

    return pred_result, query_meta_data
