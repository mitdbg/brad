import os
import pickle

from cost_model.training.test import test_one_model
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json
from cost_model.training.utils import batch_to


def auto_encode(
    model_dir,
    target_dir,
    filename_model,
    hyperparameter_path,
    workload_runs,
    statistics_file,
    with_quote=False,
    with_table_name_map=True,
):
    res, true, pred, model, test_loader = test_one_model(
        model_dir, filename_model, hyperparameter_path, workload_runs, statistics_file
    )
    run_stats = load_json(workload_runs[0])
    table_stats = run_stats.database_stats.table_stats
    if with_table_name_map:
        table_name_map = dict()
        for i in range(len(table_stats)):
            table_name_map[i] = table_stats[i].relname
        model.table_name_map = table_name_map

    all_query_feature_dict = dict()
    all_scan_feature_dict = dict()
    all_join_feature_dict = dict()
    for batch in test_loader:
        input_model, label, sample_idxs_batch, sample_idx_map = batch_to(
            batch, model.device, model.label_norm
        )
        query_feature_dict, scan_feature_dict, join_feature_dict = model.featurize(
            input_model, sample_idx_map, with_quote=with_quote
        )
        all_query_feature_dict.update(query_feature_dict)
        all_scan_feature_dict.update(scan_feature_dict)
        all_join_feature_dict.update(join_feature_dict)
    query_feature = dict()
    # for query in train_test_queries:
    for query in all_query_feature_dict:
        query_feature[str(query) + ".sql"] = dict()
        query_feature[str(query) + ".sql"]["query_feature"] = all_query_feature_dict[
            query
        ]
        query_feature[str(query) + ".sql"]["scan_feature"] = all_scan_feature_dict[
            query
        ]
        query_feature[str(query) + ".sql"]["join_feature"] = all_join_feature_dict[
            query
        ]

    with open(os.path.join(target_dir, "selected_run_query_feature.pkl"), "wb") as f:
        pickle.dump(query_feature, f)
