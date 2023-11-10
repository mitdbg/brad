# We adapted the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
import functools
from json import JSONDecodeError

import numpy as np
from sklearn import preprocessing
from sklearn.pipeline import Pipeline
from torch.utils.data import DataLoader

from workloads.cross_db_benchmark.benchmark_tools.utils import load_json
from brad.cost_model.dataset.plan_dataset import PlanDataset
from brad.cost_model.dataset.query_dataset import QueryDataset
from brad.cost_model.dataset.query_graph_batching.query_batching import query_collator
from brad.cost_model.dataset.plan_graph_batching.plan_batchers import plan_collator_dict


def read_workload_runs(
    workload_run_paths,
    read_plans=False,
    limit_queries=None,
    limit_queries_affected_wl=None,
    limit_num_tables=None,
    limit_runtime=None,
    lower_bound_num_tables=None,
    lower_bound_runtime=None,
):
    # reads several workload runs
    data = []
    database_statistics = dict()

    for i, source in enumerate(workload_run_paths):
        try:
            run = load_json(source)
        except JSONDecodeError:
            raise ValueError(f"Error reading {source}")
        database_statistics[i] = run.database_stats
        database_statistics[i].run_kwars = run.run_kwargs

        limit_per_ds = None
        if limit_queries is not None:
            if i >= len(workload_run_paths) - limit_queries_affected_wl:
                limit_per_ds = limit_queries // limit_queries_affected_wl
                print(f"Capping workload {source} after {limit_per_ds} queries")

        num_added_data = 0
        print(
            f"limit_runtime: {limit_runtime}; lower_bound_runtime: {lower_bound_runtime};"
            f"limit_num_tables: {limit_num_tables}; lower_bound_num_tables: {lower_bound_num_tables}"
        )
        if read_plans:
            for p_id, plan in enumerate(run.parsed_plans):
                if limit_runtime is not None and plan.plan_runtime > limit_runtime:
                    continue
                if (
                    lower_bound_runtime is not None
                    and plan.plan_runtime <= lower_bound_runtime
                ):
                    continue
                if limit_num_tables is not None and plan.num_tables > limit_num_tables:
                    continue
                if (
                    lower_bound_num_tables is not None
                    and plan.num_tables <= lower_bound_num_tables
                ):
                    continue
                plan.database_id = i
                data.append(plan)
                num_added_data += 1
                if limit_per_ds is not None and num_added_data > limit_per_ds:
                    print("Stopping now")
                    break
        else:
            for q_id, query in enumerate(run.parsed_queries):
                if limit_runtime is not None and query.plan_runtime > limit_runtime:
                    continue
                if (
                    lower_bound_runtime is not None
                    and query.plan_runtime <= lower_bound_runtime
                ):
                    continue
                if limit_num_tables is not None and query.num_tables > limit_num_tables:
                    continue
                if (
                    lower_bound_num_tables is not None
                    and query.num_tables <= lower_bound_num_tables
                ):
                    continue
                query.database_id = i
                data.append(query)
                num_added_data += 1
                if limit_per_ds is not None and num_added_data > limit_per_ds:
                    print("Stopping now")
                    break

    print(f"No of data points: {len(data)}")

    return data, database_statistics


def _inv_log1p(x):
    return np.exp(x) - 1


def create_datasets(
    workload_run_paths,
    read_plans=False,
    cap_training_samples=None,
    val_ratio=0.15,
    limit_queries=None,
    limit_queries_affected_wl=None,
    limit_num_tables=None,
    limit_runtime=None,
    lower_bound_num_tables=None,
    lower_bound_runtime=None,
    shuffle_before_split=True,
    loss_class_name=None,
    eval_on_test=False,
):
    """
    Creating dataset of query featurization. Set read_plans=True for plan datasets
    """
    data, database_statistics = read_workload_runs(
        workload_run_paths,
        read_plans=read_plans,
        limit_queries=limit_queries,
        limit_queries_affected_wl=limit_queries_affected_wl,
        limit_num_tables=limit_num_tables,
        limit_runtime=limit_runtime,
        lower_bound_num_tables=lower_bound_num_tables,
        lower_bound_runtime=lower_bound_runtime,
    )

    no_plans = len(data)
    plan_idxs = list(range(no_plans))
    if eval_on_test:
        # we don't need to create an evaluation dataset
        train_idxs = plan_idxs
        split_train = len(train_idxs)
    else:
        if shuffle_before_split:
            np.random.shuffle(plan_idxs)
        train_ratio = 1 - val_ratio
        split_train = int(no_plans * train_ratio)
        train_idxs = plan_idxs[:split_train]
    # Limit number of training samples. To have comparable batch sizes, replicate remaining indexes.
    if cap_training_samples is not None:
        prev_train_length = len(train_idxs)
        train_idxs = train_idxs[:cap_training_samples]
        replicate_factor = max(prev_train_length // len(train_idxs), 1)
        train_idxs = train_idxs * replicate_factor

    if read_plans:
        train_dataset = PlanDataset([data[i] for i in train_idxs], train_idxs)
    else:
        train_dataset = QueryDataset([data[i] for i in train_idxs], train_idxs)

    val_dataset = None
    if not eval_on_test:
        if val_ratio > 0:
            val_idxs = plan_idxs[split_train:]
            if read_plans:
                val_dataset = PlanDataset([data[i] for i in val_idxs], val_idxs)
            else:
                val_dataset = QueryDataset([data[i] for i in val_idxs], val_idxs)

    # derive label normalization
    runtimes = np.array([p.plan_runtime / 1000 for p in data])
    label_norm = derive_label_normalizer(loss_class_name, runtimes)

    return label_norm, train_dataset, val_dataset, database_statistics


def derive_label_normalizer(loss_class_name, y):
    if loss_class_name == "MSELoss":
        log_transformer = preprocessing.FunctionTransformer(
            np.log1p, _inv_log1p, validate=True
        )
        scale_transformer = preprocessing.MinMaxScaler()
        pipeline = Pipeline([("log", log_transformer), ("scale", scale_transformer)])
        pipeline.fit(y.reshape(-1, 1))
    elif loss_class_name == "QLoss":
        scale_transformer = preprocessing.MinMaxScaler(feature_range=(1e-2, 1))
        pipeline = Pipeline([("scale", scale_transformer)])
        pipeline.fit(y.reshape(-1, 1))
    else:
        pipeline = None
    return pipeline


def create_plan_dataloader(
    workload_run_paths,
    test_workload_run_paths,
    statistics_file,
    plan_featurization_name,
    database,
    val_ratio=0.15,
    batch_size=32,
    shuffle=True,
    num_workers=1,
    pin_memory=False,
    limit_queries=None,
    limit_queries_affected_wl=None,
    limit_num_tables=None,
    lower_bound_num_tables=None,
    lower_bound_runtime=None,
    limit_runtime=None,
    loss_class_name=None,
    eval_on_test=False,
):
    """
    Creates dataloaders that batches physical plans to train the model in a distributed fashion.
    :param workload_run_paths:
    :param val_ratio:
    :param test_ratio:
    :param batch_size:
    :param shuffle:
    :param num_workers:
    :param pin_memory:
    :return:
    """
    # split plans into train/test/validation
    label_norm, train_dataset, val_dataset, database_statistics = create_datasets(
        workload_run_paths,
        True,
        loss_class_name=loss_class_name,
        val_ratio=val_ratio,
        limit_queries=limit_queries,
        limit_queries_affected_wl=limit_queries_affected_wl,
        limit_num_tables=limit_num_tables,
        limit_runtime=limit_runtime,
        lower_bound_num_tables=lower_bound_num_tables,
        lower_bound_runtime=lower_bound_runtime,
        eval_on_test=eval_on_test,
    )

    # postgres_plan_collator does the heavy lifting of creating the graphs and extracting the features and thus requires both
    # database statistics but also feature statistics
    feature_statistics = load_json(statistics_file, namespace=False)

    plan_collator = plan_collator_dict[database]
    train_collate_fn = functools.partial(
        plan_collator,
        database=database,
        db_statistics=database_statistics,
        feature_statistics=feature_statistics,
        plan_featurization_name=plan_featurization_name,
    )
    dataloader_args = dict(
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        collate_fn=train_collate_fn,
        pin_memory=pin_memory,
    )
    train_loader = DataLoader(train_dataset, **dataloader_args)
    if val_dataset is not None:
        val_loader = DataLoader(val_dataset, **dataloader_args)
    else:
        val_loader = None

    # for each test workoad run create a distinct test loader
    test_loaders = None
    if test_workload_run_paths is not None:
        test_loaders = []
        for p in test_workload_run_paths:
            _, test_dataset, _, test_database_statistics = create_datasets(
                [p],
                True,
                loss_class_name=loss_class_name,
                val_ratio=0.0,
                shuffle_before_split=False,
            )
            # test dataset
            test_collate_fn = functools.partial(
                plan_collator,
                database=database,
                db_statistics=test_database_statistics,
                feature_statistics=feature_statistics,
                plan_featurization_name=plan_featurization_name,
            )
            # previously shuffle=False but this resulted in bugs
            dataloader_args.update(collate_fn=test_collate_fn)
            test_loader = DataLoader(test_dataset, **dataloader_args)
            test_loaders.append(test_loader)
    if eval_on_test:
        _, val_dataset, _, val_database_statistics = create_datasets(
            test_workload_run_paths,
            True,
            loss_class_name=loss_class_name,
            val_ratio=0.0,
            shuffle_before_split=False,
        )
        val_collate_fn = functools.partial(
            plan_collator,
            database=database,
            db_statistics=val_database_statistics,
            feature_statistics=feature_statistics,
            plan_featurization_name=plan_featurization_name,
        )
        dataloader_args = dict(
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
            collate_fn=val_collate_fn,
            pin_memory=pin_memory,
        )
        val_loader = DataLoader(val_dataset, **dataloader_args)

    return label_norm, feature_statistics, train_loader, val_loader, test_loaders


def create_query_dataloader(
    workload_run_paths,
    test_workload_run_paths,
    statistics_file,
    query_featurization_name,
    database,
    val_ratio=0.15,
    batch_size=32,
    shuffle=True,
    num_workers=1,
    pin_memory=False,
    limit_queries=None,
    limit_queries_affected_wl=None,
    limit_num_tables=None,
    lower_bound_num_tables=None,
    lower_bound_runtime=None,
    limit_runtime=None,
    loss_class_name=None,
    eval_on_test=False,
):
    """
    Creates dataloaders that batches query featurization to train the model in a distributed fashion.
    """
    # split plans into train/test/validation
    label_norm, train_dataset, val_dataset, database_statistics = create_datasets(
        workload_run_paths,
        False,
        loss_class_name=loss_class_name,
        val_ratio=val_ratio,
        limit_queries=limit_queries,
        limit_queries_affected_wl=limit_queries_affected_wl,
        limit_num_tables=limit_num_tables,
        limit_runtime=limit_runtime,
        lower_bound_num_tables=lower_bound_num_tables,
        lower_bound_runtime=lower_bound_runtime,
        eval_on_test=eval_on_test,
    )

    # postgres_plan_collator does the heavy lifting of creating the graphs and extracting the features and thus requires both
    # database statistics but also feature statistics
    feature_statistics = load_json(statistics_file, namespace=False)

    train_collate_fn = functools.partial(
        query_collator,
        database=database,
        db_statistics=database_statistics,
        feature_statistics=feature_statistics,
        query_featurization_name=query_featurization_name,
    )
    dataloader_args = dict(
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        collate_fn=train_collate_fn,
        pin_memory=pin_memory,
    )
    train_loader = DataLoader(train_dataset, **dataloader_args)
    if val_dataset is not None:
        val_loader = DataLoader(val_dataset, **dataloader_args)
    else:
        val_loader = None

    # for each test workoad run create a distinct test loader
    test_loaders = None
    if test_workload_run_paths is not None:
        test_loaders = []
        for p in test_workload_run_paths:
            _, test_dataset, _, test_database_statistics = create_datasets(
                [p],
                False,
                loss_class_name=loss_class_name,
                val_ratio=0.0,
                shuffle_before_split=False,
            )
            # test dataset
            test_collate_fn = functools.partial(
                query_collator,
                database=database,
                db_statistics=test_database_statistics,
                feature_statistics=feature_statistics,
                query_featurization_name=query_featurization_name,
            )
            # previously shuffle=False but this resulted in bugs
            dataloader_args.update(collate_fn=test_collate_fn)
            test_loader = DataLoader(test_dataset, **dataloader_args)
            test_loaders.append(test_loader)

    if eval_on_test:
        _, val_dataset, _, val_database_statistics = create_datasets(
            test_workload_run_paths,
            False,
            loss_class_name=loss_class_name,
            val_ratio=0.0,
            shuffle_before_split=False,
        )
        val_collate_fn = functools.partial(
            query_collator,
            database=database,
            db_statistics=val_database_statistics,
            feature_statistics=feature_statistics,
            query_featurization_name=query_featurization_name,
        )
        dataloader_args = dict(
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=num_workers,
            collate_fn=val_collate_fn,
            pin_memory=pin_memory,
        )
        val_loader = DataLoader(val_dataset, **dataloader_args)

    return label_norm, feature_statistics, train_loader, val_loader, test_loaders


def create_dataloader(
    workload_run_paths,
    test_workload_run_paths,
    statistics_file,
    featurization_name,
    database,
    val_ratio=0.15,
    batch_size=32,
    shuffle=True,
    num_workers=1,
    pin_memory=False,
    limit_queries=None,
    limit_queries_affected_wl=None,
    limit_num_tables=None,
    lower_bound_num_tables=None,
    lower_bound_runtime=None,
    limit_runtime=None,
    loss_class_name=None,
    is_query=True,
    eval_on_test=False,
):
    if is_query:
        return create_query_dataloader(
            workload_run_paths,
            test_workload_run_paths,
            statistics_file,
            featurization_name,
            database,
            val_ratio,
            batch_size,
            shuffle,
            num_workers,
            pin_memory,
            limit_queries,
            limit_queries_affected_wl,
            limit_num_tables,
            lower_bound_num_tables,
            lower_bound_runtime,
            limit_runtime,
            loss_class_name,
            eval_on_test,
        )
    else:
        return create_plan_dataloader(
            workload_run_paths,
            test_workload_run_paths,
            statistics_file,
            featurization_name,
            database,
            val_ratio,
            batch_size,
            shuffle,
            num_workers,
            pin_memory,
            limit_queries,
            limit_queries_affected_wl,
            limit_num_tables,
            lower_bound_num_tables,
            lower_bound_runtime,
            limit_runtime,
            loss_class_name,
            eval_on_test,
        )


def create_dataloader_for_brad(
    database,
    query_idx,
    parsed_runs,
    database_statistics,
    feature_statistics,
    query_featurization_name,
    batch_size=256,
    num_workers=1,
    pin_memory=False,
):
    data = [parsed_runs["parsed_queries"][i] for i in query_idx]
    plan_idxs = list(range(len(data)))
    dataset = QueryDataset(data, plan_idxs)
    train_collate_fn = functools.partial(
        query_collator,
        database=database,
        db_statistics=database_statistics,
        feature_statistics=feature_statistics,
        query_featurization_name=query_featurization_name,
    )
    dataloader_args = dict(
        batch_size=batch_size,
        shuffle=False,
        num_workers=num_workers,
        collate_fn=train_collate_fn,
        pin_memory=pin_memory,
    )
    loader = DataLoader(dataset, **dataloader_args)
    return loader
