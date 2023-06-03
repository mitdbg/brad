import time
import os
import numpy as np
import torch
import torch.optim as opt
from tqdm import tqdm

from workloads.cross_db_benchmark.benchmark_tools.utils import load_json
from brad.cost_model.dataset.dataset_creation import create_dataloader
from brad.cost_model.training.checkpoint import load_checkpoint
from brad.cost_model.training.metrics import MAPE, RMSE, QError
from brad.cost_model.training.utils import batch_to, flatten_dict
from brad.cost_model.encoder.specific_models.model import zero_shot_models


def training_model_loader(
    workload_runs,
    test_workload_runs,
    statistics_file,
    target_dir,
    filename_model,
    optimizer_class_name="Adam",
    optimizer_kwargs=None,
    final_mlp_kwargs=None,
    node_type_kwargs=None,
    model_kwargs=None,
    tree_layer_name="GATConv",
    tree_layer_kwargs=None,
    hidden_dim=32,
    batch_size=32,
    output_dim=1,
    epochs=0,
    device="cpu",
    plan_featurization_name=None,
    max_epoch_tuples=100000,
    param_dict=None,
    num_workers=1,
    early_stopping_patience=20,
    trial=None,
    database=None,
    limit_queries=None,
    limit_queries_affected_wl=None,
    skip_train=False,
    seed=0,
):
    if model_kwargs is None:
        model_kwargs = dict()

    # seed for reproducibility
    torch.manual_seed(seed)
    np.random.seed(seed)

    target_test_csv_paths = []
    if test_workload_runs is not None:
        for p in test_workload_runs:
            test_workload = os.path.basename(p).replace(".json", "")
            target_test_csv_paths.append(
                os.path.join(target_dir, f"test_{filename_model}_{test_workload}.csv")
            )

    # create a dataset
    loss_class_name = final_mlp_kwargs["loss_class_name"]
    (
        label_norm,
        feature_statistics,
        train_loader,
        val_loader,
        test_loaders,
    ) = create_dataloader(
        workload_runs,
        test_workload_runs,
        statistics_file,
        plan_featurization_name,
        database,
        val_ratio=0.15,
        batch_size=batch_size,
        shuffle=True,
        num_workers=num_workers,
        pin_memory=False,
        limit_queries=limit_queries,
        limit_queries_affected_wl=limit_queries_affected_wl,
        loss_class_name=loss_class_name,
    )

    if loss_class_name == "QLoss":
        metrics = [
            RMSE(),
            MAPE(),
            QError(percentile=50, early_stopping_metric=True),
            QError(percentile=95),
            QError(percentile=100),
        ]
    elif loss_class_name == "MSELoss":
        metrics = [
            RMSE(early_stopping_metric=True),
            MAPE(),
            QError(percentile=50),
            QError(percentile=95),
            QError(percentile=100),
        ]

    # create zero shot model dependent on database
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
        label_norm=label_norm,
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
        target_dir,
        filename_model,
        optimizer=optimizer,
        metrics=metrics,
        filetype=".pt",
    )
    return test_loaders, model


def load_model(
    workload_runs,
    test_workload_runs,
    statistics_file,
    target_dir,
    filename_model,
    hyperparameter_path,
    device="cpu",
    max_epoch_tuples=100000,
    num_workers=1,
    loss_class_name="QLoss",
    database=None,
    seed=0,
    limit_queries=None,
    limit_queries_affected_wl=None,
    max_no_epochs=None,
    skip_train=False,
):
    """
    Reads out hyperparameters and trains model
    """
    print(f"Reading hyperparameters from {hyperparameter_path}")
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
        epochs=200 if max_no_epochs is None else max_no_epochs,
        early_stopping_patience=20,
        max_epoch_tuples=max_epoch_tuples,
        batch_size=hyperparams.pop("batch_size"),
        device=device,
        num_workers=num_workers,
        seed=seed,
        limit_queries=limit_queries,
        limit_queries_affected_wl=limit_queries_affected_wl,
        skip_train=skip_train,
    )

    assert len(hyperparams) == 0, (
        f"Not all hyperparams were used (not used: {hyperparams.keys()}). Hence generation "
        f"and reading does not seem to fit"
    )

    param_dict = flatten_dict(train_kwargs)

    test_loaders, model = training_model_loader(
        workload_runs,
        test_workload_runs,
        statistics_file,
        target_dir,
        filename_model,
        param_dict=param_dict,
        database=database,
        **train_kwargs,
    )

    return test_loaders, model


def validate_model(
    val_loader,
    model,
    epoch=0,
    epoch_stats=None,
    metrics=None,
    max_epoch_tuples=None,
    custom_batch_to=batch_to,
    verbose=False,
    log_all_queries=False,
):
    model.eval()
    print(model.plan_featurization_name)
    with torch.autograd.no_grad():
        val_loss = torch.Tensor([0])
        labels = []
        preds = []
        probs = []
        sample_idxs = []

        # evaluate test set using model
        test_start_t = time.perf_counter()
        val_num_tuples = 0
        for batch_idx, batch in enumerate(tqdm(val_loader)):
            if (
                max_epoch_tuples is not None
                and batch_idx * val_loader.batch_size > max_epoch_tuples
            ):
                break
            val_num_tuples += val_loader.batch_size

            input_model, label, sample_idxs_batch, sample_idx_map = custom_batch_to(
                batch, model.device, model.label_norm
            )
            # sample_idxs += sample_idxs_batch
            output = model(input_model)

            # sum up mean batch losses
            val_loss += model.loss_fxn(output, label).cpu()

            # inverse transform the predictions and labels
            curr_pred = output.cpu().numpy()
            curr_label = label.cpu().numpy()
            if model.label_norm is not None:
                curr_pred = model.label_norm.inverse_transform(curr_pred)
                curr_label = model.label_norm.inverse_transform(
                    curr_label.reshape(-1, 1)
                )
                curr_label = curr_label.reshape(-1)

            preds.append(curr_pred.reshape(-1))
            labels.append(curr_label.reshape(-1))

        if epoch_stats is not None:
            epoch_stats.update(val_time=time.perf_counter() - test_start_t)
            epoch_stats.update(val_num_tuples=val_num_tuples)
            val_loss = (val_loss.cpu() / len(val_loader)).item()
            print(f"val_loss epoch {epoch}: {val_loss}")
            epoch_stats.update(val_loss=val_loss)

        labels = np.concatenate(labels, axis=0)
        preds = np.concatenate(preds, axis=0)
        return labels, preds


def test_one_model(
    database,
    target_dir,
    filename_model,
    hyperparameter_path,
    test_workload_runs,
    statistics_file,
):
    test_loaders, model = load_model(
        test_workload_runs,
        test_workload_runs,
        statistics_file,
        target_dir,
        filename_model,
        hyperparameter_path,
        database=database,
    )
    true, pred = validate_model(test_loaders[0], model)
    qerror = np.maximum(true / pred, pred / true)
    res = []
    for i in [50, 95, 99]:
        print(f"{i} percentile is {np.percentile(qerror, i)}")
        res.append(np.percentile(qerror, i))
    return res, true, pred, model, test_loaders[0]
