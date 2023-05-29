import collections
import dgl
import numpy as np
import torch
from sklearn.preprocessing import RobustScaler

from workloads.cross_db_benchmark.benchmark_tools.database import DatabaseSystem
from workloads.cross_db_benchmark.benchmark_tools.generate_workload import Operator
from cost_model.dataset.query_featurization import aurora_query_featurization
from cost_model.dataset.query_featurization import postgres_query_featurizations
from cost_model.dataset.query_featurization import redshift_query_featurization
from cost_model.dataset.query_featurization import athena_query_featurization
from cost_model.preprocessing.feature_statistics import FeatureType


def encode(column, plan_params, feature_statistics):
    #  fallback in case actual cardinality is not in plan parameters
    if column == "act_card" and column not in plan_params:
        value = 0
    else:
        value = plan_params[column]
    if feature_statistics[column].get("type") == str(FeatureType.numeric):
        enc_value = (
            feature_statistics[column]["scaler"].transform(np.array([[value]])).item()
        )
    elif feature_statistics[column].get("type") == str(FeatureType.categorical):
        value_dict = feature_statistics[column]["value_dict"]
        enc_value = value_dict[str(value)]
    elif feature_statistics[column].get("type") == str(FeatureType.boolean):
        enc_value = value
    else:
        raise NotImplementedError
    return enc_value


def parse_join_nodes_to_graph(
    join_nodes,
    sample_join_map,
    sample_jfp_map,
    database_id,
    plan_depths,
    db_statistics,
    feature_statistics,
    filter_column_idx,
    join_features,
    join_to_encode_edges,
    filter_to_join_edges,
    filter_features,
    query_featurization,
    predicate_depths,
    intra_predicate_edges,
    logical_preds,
    parent_node_id=0,
    depth=1,
):
    joined_tables = dict()
    use_alias = False
    for join_node in join_nodes:
        join_node_id = len(plan_depths)
        if hasattr(join_node, "table_alias") and join_node.table_alias != [None, None]:
            use_alias = True
        if use_alias:
            join_table_identifier = tuple(sorted(join_node.table_alias))
        else:
            join_table_identifier = tuple(sorted(join_node.tables))
        sample_join_map[join_table_identifier] = len(join_features)

        plan_depths.append(depth)
        plan_params = vars(join_node.plan_parameters)
        curr_join_features = [
            encode(column, plan_params, feature_statistics)
            for column in query_featurization.JOIN_FEATURES
        ]
        join_features.append(curr_join_features)

        filter_column = join_node.filter_columns
        db_column_features = db_statistics[database_id].column_stats

        parse_join_predicates(
            sample_jfp_map,
            join_table_identifier,
            db_column_features,
            feature_statistics,
            filter_column,
            filter_to_join_edges,
            filter_column_idx,
            query_featurization,
            filter_features,
            predicate_depths,
            intra_predicate_edges,
            logical_preds,
            join_node_id=join_node_id,
        )

        join_to_encode_edges.append((join_node_id, parent_node_id))
        if use_alias:
            joined_tables[join_node_id] = join_node.table_alias
        else:
            joined_tables[join_node_id] = join_node.tables
    return joined_tables, use_alias


def parse_scan_nodes_to_graph(
    scan_nodes,
    sample_scan_map,
    database_id,
    plan_depths,
    db_statistics,
    feature_statistics,
    joined_tables,
    use_alias,
    filter_column_idx,
    scan_features,
    scan_to_join_edges,
    filter_to_scan_edges,
    filter_features,
    output_column_to_scan_edges,
    output_column_features,
    column_to_output_column_edges,
    column_features,
    table_features,
    table_to_scan_edges,
    query_featurization,
    predicate_depths,
    intra_predicate_edges,
    logical_preds,
    output_column_idx,
    column_idx,
    table_idx,
    depth=2,
):
    scan_nodes = vars(scan_nodes)
    for scan_table in scan_nodes:
        scan_node = scan_nodes[scan_table]

        scan_node_id = len(plan_depths)
        sample_scan_map[scan_table] = (len(scan_features), scan_node.table)
        plan_depths.append(depth)
        plan_params = vars(scan_node.plan_parameters)
        curr_scan_features = [
            encode(column, plan_params, feature_statistics)
            for column in query_featurization.SCAN_FEATURES
        ]
        scan_features.append(curr_scan_features)

        if scan_node.output_columns is not None:
            for output_column in scan_node.output_columns:
                output_column_node_id = output_column_idx.get(
                    (
                        output_column.aggregation,
                        tuple(output_column.columns),
                        database_id,
                    )
                )

                # if not, create
                if output_column_node_id is None:
                    curr_output_column_features = [
                        encode(column, vars(output_column), feature_statistics)
                        for column in query_featurization.OUTPUT_COLUMN_FEATURES
                    ]

                    output_column_node_id = len(output_column_features)
                    output_column_features.append(curr_output_column_features)
                    output_column_idx[
                        (
                            output_column.aggregation,
                            tuple(output_column.columns),
                            database_id,
                        )
                    ] = output_column_node_id

                    # featurize product of columns if there are any
                    db_column_features = db_statistics[database_id].column_stats
                    for column in output_column.columns:
                        column_node_id = column_idx.get((column, database_id))
                        if column_node_id is None:
                            curr_column_features = [
                                encode(
                                    feature_name,
                                    vars(db_column_features[column]),
                                    feature_statistics,
                                )
                                for feature_name in query_featurization.COLUMN_FEATURES
                            ]
                            column_node_id = len(column_features)
                            column_features.append(curr_column_features)
                            column_idx[(column, database_id)] = column_node_id
                        column_to_output_column_edges.append(
                            (column_node_id, output_column_node_id)
                        )

                # in any case add the corresponding edge
                output_column_to_scan_edges.append(
                    (output_column_node_id, scan_node_id)
                )

        if scan_node.filter_columns is not None:
            db_column_features = db_statistics[database_id].column_stats

            # check if node already exists in the graph
            # filter_node_id = fitler_node_idx.get((filter_column.operator, filter_column.column, database_id))

            parse_predicates(
                db_column_features,
                feature_statistics,
                filter_column_idx,
                scan_node.filter_columns,
                filter_to_scan_edges,
                query_featurization,
                filter_features,
                predicate_depths,
                intra_predicate_edges,
                logical_preds,
                plan_node_id=scan_node_id,
            )
        if not use_alias:
            scan_table_id = int(scan_table)
        else:
            scan_table_id = scan_node.table
        table_node_id = table_idx.get((scan_table_id, database_id))
        db_table_statistics = db_statistics[database_id].table_stats

        if table_node_id is None:
            curr_table_features = [
                encode(
                    feature_name,
                    vars(db_table_statistics[scan_table_id]),
                    feature_statistics,
                )
                for feature_name in query_featurization.TABLE_FEATURES
            ]
            table_node_id = len(table_features)
            table_features.append(curr_table_features)
            table_idx[(scan_table_id, database_id)] = table_node_id
        table_to_scan_edges.append((table_node_id, scan_node_id))

        # adding parent
        for join_node_id in joined_tables:
            tables = joined_tables[join_node_id]
            if use_alias and scan_table in tables:
                scan_to_join_edges.append((scan_node_id, join_node_id))
            elif not use_alias and scan_table_id in tables:
                scan_to_join_edges.append((scan_node_id, join_node_id))


def query_to_graph(
    root,
    database_id,
    sample_idx_map,
    filter_column_idx,
    plan_depths,
    db_statistics,
    feature_statistics,
    encode_features,
    join_features,
    scan_features,
    join_to_encode_edges,
    scan_to_join_edges,
    filter_to_scan_edges,
    filter_to_join_edges,
    filter_features,
    output_column_to_encode_edges,
    output_column_to_scan_edges,
    output_column_features,
    column_to_output_column_edges,
    column_features,
    table_features,
    table_to_scan_edges,
    output_column_idx,
    column_idx,
    table_idx,
    query_featurization,
    predicate_depths,
    intra_predicate_edges,
    logical_preds,
    parent_node_id=None,
    depth=0,
):
    # plan only contains three depth: encode (0), join (1), and scan (2)
    encode_node_id = len(plan_depths)
    plan_depths.append(depth)
    sample_idx_map["encode"] = len(encode_features)
    sample_idx_map["scan"] = dict()
    sample_idx_map["join"] = dict()
    sample_idx_map["join_predicate"] = dict()

    # add encode features
    plan_params = vars(root.plan_parameters)
    curr_encode_features = [
        encode(column, plan_params, feature_statistics)
        for column in query_featurization.ENCODE_FEATURES
    ]
    encode_features.append(curr_encode_features)

    # encode output columns which can in turn have several columns as a product in the aggregation
    output_columns = plan_params.get("output_columns")
    if output_columns is not None:
        for output_column in output_columns:
            output_column_node_id = output_column_idx.get(
                (output_column.aggregation, tuple(output_column.columns), database_id)
            )

            # if not, create
            if output_column_node_id is None:
                curr_output_column_features = [
                    encode(column, vars(output_column), feature_statistics)
                    for column in query_featurization.OUTPUT_COLUMN_FEATURES
                ]

                output_column_node_id = len(output_column_features)
                output_column_features.append(curr_output_column_features)
                output_column_idx[
                    (
                        output_column.aggregation,
                        tuple(output_column.columns),
                        database_id,
                    )
                ] = output_column_node_id

                # featurize product of columns if there are any
                db_column_features = db_statistics[database_id].column_stats
                for column in output_column.columns:
                    column_node_id = column_idx.get((column, database_id))
                    if column_node_id is None:
                        curr_column_features = [
                            encode(
                                feature_name,
                                vars(db_column_features[column]),
                                feature_statistics,
                            )
                            for feature_name in query_featurization.COLUMN_FEATURES
                        ]
                        column_node_id = len(column_features)
                        column_features.append(curr_column_features)
                        column_idx[(column, database_id)] = column_node_id
                    column_to_output_column_edges.append(
                        (column_node_id, output_column_node_id)
                    )

            # in any case add the corresponding edge
            output_column_to_encode_edges.append(
                (output_column_node_id, encode_node_id)
            )

    joined_tables, use_alias = parse_join_nodes_to_graph(
        root.join_nodes,
        sample_idx_map["join"],
        sample_idx_map["join_predicate"],
        database_id,
        plan_depths,
        db_statistics,
        feature_statistics,
        filter_column_idx,
        join_features,
        join_to_encode_edges,
        filter_to_join_edges,
        filter_features,
        query_featurization,
        predicate_depths,
        intra_predicate_edges,
        logical_preds,
        parent_node_id=encode_node_id,
        depth=1,
    )

    parse_scan_nodes_to_graph(
        root.scan_nodes,
        sample_idx_map["scan"],
        database_id,
        plan_depths,
        db_statistics,
        feature_statistics,
        joined_tables,
        use_alias,
        filter_column_idx,
        scan_features,
        scan_to_join_edges,
        filter_to_scan_edges,
        filter_features,
        output_column_to_scan_edges,
        output_column_features,
        column_to_output_column_edges,
        column_features,
        table_features,
        table_to_scan_edges,
        query_featurization,
        predicate_depths,
        intra_predicate_edges,
        logical_preds,
        output_column_idx,
        column_idx,
        table_idx,
        depth=2,
    )


def parse_join_predicates(
    sample_jfp_map,
    joined_tables,
    db_column_features,
    feature_statistics,
    filter_column,
    filter_to_join_edges,
    filter_column_idx,
    plan_featurization,
    filter_features,
    predicate_depths,
    intra_predicate_edges,
    logical_preds,
    join_node_id=0,
    depth=0,
):
    """
    Parse the join predicate columns: here we assume join predicate has the form A.id = (or <, >) B.id
    """
    filter_node_id = len(predicate_depths)
    predicate_depths.append(depth)
    predicate_depths.append(depth)

    curr_filter_features = [
        encode(feature_name, vars(filter_column), feature_statistics)
        for feature_name in plan_featurization.FILTER_FEATURES
    ]

    assert (
        len(filter_column.columns) == 2
    ), "currently only supporting join between two keys"

    sample_jfp_map[tuple(sorted(joined_tables))] = filter_column_idx[0]
    for col in filter_column.columns:
        curr_filter_col_feats = [
            encode(column, vars(db_column_features[col]), feature_statistics)
            for column in plan_featurization.COLUMN_FEATURES
        ]
        logical_preds.append(False)
        filter_column_idx[0] += 1
        filter_features.append(curr_filter_features + curr_filter_col_feats)
        filter_to_join_edges.append((filter_node_id, join_node_id))
        filter_node_id += 1


def parse_predicates(
    db_column_features,
    feature_statistics,
    filter_column_idx,
    filter_column,
    filter_to_plan_edges,
    plan_featurization,
    predicate_col_features,
    predicate_depths,
    intra_predicate_edges,
    logical_preds,
    plan_node_id=None,
    parent_filter_node_id=None,
    depth=0,
):
    """
    Recursive parsing of predicate columns

    :param db_column_features:
    :param feature_statistics:
    :param filter_column:
    :param filter_to_plan_edges:
    :param plan_featurization:
    :param plan_node_id:
    :param predicate_col_features:
    :return:
    """
    filter_node_id = len(predicate_depths)
    predicate_depths.append(depth)

    # gather features
    if filter_column.operator in {str(op) for op in list(Operator)}:
        curr_filter_features = [
            encode(feature_name, vars(filter_column), feature_statistics)
            for feature_name in plan_featurization.FILTER_FEATURES
        ]

        if filter_column.column is not None:
            curr_filter_col_feats = [
                encode(
                    column,
                    vars(db_column_features[filter_column.column]),
                    feature_statistics,
                )
                for column in plan_featurization.COLUMN_FEATURES
            ]
        # hack for cases in which we have no base filter column (e.g., in a having clause where the column is some
        # result column of a subquery/groupby). In the future, this should be replaced by some graph model that also
        # encodes the structure of this output column
        else:
            curr_filter_col_feats = [0 for _ in plan_featurization.COLUMN_FEATURES]
        curr_filter_features += curr_filter_col_feats
        logical_preds.append(False)
        filter_column_idx[0] += 1

    else:
        curr_filter_features = [
            encode(feature_name, vars(filter_column), feature_statistics)
            for feature_name in plan_featurization.FILTER_FEATURES
        ]
        logical_preds.append(True)

    predicate_col_features.append(curr_filter_features)

    # add edge either to plan or inside predicates
    if depth == 0:
        assert plan_node_id is not None
        # in any case add the corresponding edge
        filter_to_plan_edges.append((filter_node_id, plan_node_id))

    else:
        assert parent_filter_node_id is not None
        intra_predicate_edges.append((filter_node_id, parent_filter_node_id))

    # recurse
    for c in filter_column.children:
        parse_predicates(
            db_column_features,
            feature_statistics,
            filter_column_idx,
            c,
            filter_to_plan_edges,
            plan_featurization,
            predicate_col_features,
            predicate_depths,
            intra_predicate_edges,
            logical_preds,
            parent_filter_node_id=filter_node_id,
            depth=depth + 1,
        )


def query_collator(
    plans,
    database,
    feature_statistics=None,
    db_statistics=None,
    query_featurization_name=None,
):
    """
    Combines logical query representation into a large graph that can be fed into ML models.
    """
    # readout how to featurize join plans
    if database == DatabaseSystem.POSTGRES:
        plan_featurization = postgres_query_featurizations.__dict__[
            query_featurization_name
        ]
    elif database == DatabaseSystem.AURORA:
        plan_featurization = aurora_query_featurization.__dict__[
            query_featurization_name
        ]
    elif database == DatabaseSystem.REDSHIFT:
        plan_featurization = redshift_query_featurization.__dict__[
            query_featurization_name
        ]
    elif database == DatabaseSystem.ATHENA:
        plan_featurization = athena_query_featurization.__dict__[
            query_featurization_name
        ]
    else:
        raise NotImplementedError

    # output:
    #   - list of labels (i.e., plan runtimes)
    #   - feature dictionaries
    #       - encoding_features: matrix
    #       - column_features: matrix
    #       - filter_column_features: matrix
    #       - scan_node_features: matrix
    #       - join_node_features: matrix
    #       - table_features: matrix
    #       - logical_pred_features: matrix
    #   - edges
    #       - table_to_scan
    #       - table_to_column
    #       - column_to_pred
    #       - column_to_scan
    #       - pred_to_scan
    #       - pred_to_join
    #       - scan_to_join
    #       - join_to_encode
    #       - intra predicate (e.g., column to AND)
    plan_depths = []
    encode_features = []
    join_features = []
    scan_features = []
    join_to_encode_edges = []
    scan_to_join_edges = []
    filter_to_scan_edges = []
    filter_to_join_edges = []
    filter_features = []
    output_column_to_encode_edges = []
    output_column_to_scan_edges = []
    output_column_features = []
    column_to_output_column_edges = []
    column_features = []
    table_features = []
    table_to_scan_edges = []
    labels = []
    predicate_depths = []
    intra_predicate_edges = []
    logical_preds = []

    output_column_idx = dict()
    column_idx = dict()
    table_idx = dict()
    sample_idx_map = dict()

    # prepare robust encoder for the numerical fields
    add_numerical_scalers(feature_statistics)

    # iterate over plans and create lists of edges and features per node
    sample_idxs = []
    filter_column_idx = [0]
    for sample_idx, p in plans:
        sample_idxs.append(sample_idx)
        labels.append(p.plan_runtime)
        sample_idx_map[sample_idx] = dict()
        query_to_graph(
            p,
            p.database_id,
            sample_idx_map[sample_idx],
            filter_column_idx,
            plan_depths,
            db_statistics,
            feature_statistics,
            encode_features,
            join_features,
            scan_features,
            join_to_encode_edges,
            scan_to_join_edges,
            filter_to_scan_edges,
            filter_to_join_edges,
            filter_features,
            output_column_to_encode_edges,
            output_column_to_scan_edges,
            output_column_features,
            column_to_output_column_edges,
            column_features,
            table_features,
            table_to_scan_edges,
            output_column_idx,
            column_idx,
            table_idx,
            plan_featurization,
            predicate_depths,
            intra_predicate_edges,
            logical_preds,
        )

    assert len(labels) == len(plans)
    assert len(plan_depths) == len(encode_features) + len(scan_features) + len(
        join_features
    )

    data_dict, nodes_per_depth, plan_dict = create_node_types_per_depth(
        plan_depths, join_to_encode_edges, scan_to_join_edges
    )

    # similarly create node types:
    #   pred_node_{depth}, filter column
    pred_dict = dict()
    nodes_per_pred_depth = collections.defaultdict(int)
    no_filter_columns = 0
    for pred_node, d in enumerate(predicate_depths):
        # predicate node
        if logical_preds[pred_node]:
            pred_dict[pred_node] = (nodes_per_pred_depth[d], d)
            nodes_per_pred_depth[d] += 1
        # filter column
        else:
            pred_dict[pred_node] = no_filter_columns
            no_filter_columns += 1

    adapt_predicate_edges(
        data_dict,
        filter_to_join_edges,
        filter_to_scan_edges,
        intra_predicate_edges,
        logical_preds,
        plan_dict,
        pred_dict,
        pred_node_type_id,
    )

    # we additionally have filters, tables, columns, output_columns and plan nodes as node types
    data_dict[
        ("column", "col_output_col", "output_column")
    ] = column_to_output_column_edges
    for u, v in output_column_to_scan_edges:
        v_node_id = plan_dict[v]
        data_dict[("output_column", "to_scan", f"scan")].append((u, v_node_id))
    for u, v in table_to_scan_edges:
        v_node_id = plan_dict[v]
        data_dict[("table", "to_scan", "scan")].append((u, v_node_id))

    # also pass number of nodes per type
    max_depth, max_pred_depth = get_depths(plan_depths, predicate_depths)
    num_nodes_dict = {
        "column": len(column_features),
        "table": len(table_features),
        "output_column": len(output_column_features),
        "filter_column": len(logical_preds) - sum(logical_preds),
    }
    assert (
        num_nodes_dict["filter_column"] == filter_column_idx[0]
    ), f"mismatch {num_nodes_dict['filter_column']} and {filter_column_idx[0]}"
    num_nodes_dict = update_node_counts(
        max_pred_depth, nodes_per_depth, nodes_per_pred_depth, num_nodes_dict
    )

    # create graph
    graph = dgl.heterograph(data_dict, num_nodes_dict=num_nodes_dict)
    graph.max_depth = max_depth
    graph.max_pred_depth = max_pred_depth

    features = collections.defaultdict(list)
    features.update(
        dict(
            column=column_features,
            table=table_features,
            output_column=output_column_features,
            filter_column=[
                f for f, log_pred in zip(filter_features, logical_preds) if not log_pred
            ],
            encode=encode_features,
            join=join_features,
            scan=scan_features,
        )
    )

    # sort the predicate features based on the depth
    for pred_node_id, pred_feat in enumerate(filter_features):
        if not logical_preds[pred_node_id]:
            continue
        node_type, _ = pred_node_type_id(logical_preds, pred_dict, pred_node_id)
        features[node_type].append(pred_feat)

    features = postprocess_feats(features, num_nodes_dict)

    # rather deal with runtimes in secs
    labels = postprocess_labels(labels)

    return graph, features, labels, sample_idxs, sample_idx_map


def postprocess_labels(labels):
    labels = np.array(labels, dtype=np.float32)
    labels /= 1000
    # we do this later
    # labels = torch.from_numpy(labels)
    return labels


def postprocess_feats(features, num_nodes_dict):
    # convert to tensors, replace nan with 0
    for k in features.keys():
        v = features[k]
        shape = np.array([len(v[i]) for i in range(len(v))])
        if np.min(shape) < np.max(shape):
            # need to pad the feature
            v = np.zeros((len(v), np.max(shape)), dtype=np.float32)
            for i in range(len(v)):
                v[i, 0 : len(v[i])] = v[i]
        else:
            v = np.array(v, dtype=np.float32)
        v = np.nan_to_num(v, nan=0.0)
        v = torch.from_numpy(v)
        features[k] = v
    # filter out any node type with zero nodes
    features = {k: v for k, v in features.items() if k in num_nodes_dict}
    return features


def update_node_counts(
    max_pred_depth, nodes_per_depth, nodes_per_pred_depth, num_nodes_dict
):
    num_nodes_dict["encode"] = nodes_per_depth[0]
    num_nodes_dict["join"] = nodes_per_depth[1]
    num_nodes_dict["scan"] = nodes_per_depth[2]
    num_nodes_dict.update(
        {
            f"logical_pred_{d}": nodes_per_pred_depth[d]
            for d in range(max_pred_depth + 1)
        }
    )
    # filter out any node type with zero nodes
    num_nodes_dict = {k: v for k, v in num_nodes_dict.items() if v > 0}
    return num_nodes_dict


def get_depths(plan_depths, predicate_depths):
    max_depth = max(plan_depths)
    max_pred_depth = 0
    if len(predicate_depths) > 0:
        max_pred_depth = max(predicate_depths)
    return max_depth, max_pred_depth


def adapt_predicate_edges(
    data_dict,
    filter_to_join_edges,
    filter_to_scan_edges,
    intra_predicate_edges,
    logical_preds,
    plan_dict,
    pred_dict,
    pred_node_type_id_func,
):
    # convert to plan edges
    for u, v in filter_to_join_edges:
        # transform plan node to right id and depth
        v_node_id = plan_dict[v]
        # transform predicate node to right node type and id
        node_type, u_node_id = pred_node_type_id_func(logical_preds, pred_dict, u)
        data_dict[(node_type, "to_join", f"join")].append((u_node_id, v_node_id))

    for u, v in filter_to_scan_edges:
        # transform plan node to right id and depth
        v_node_id = plan_dict[v]
        # transform predicate node to right node type and id
        node_type, u_node_id = pred_node_type_id_func(logical_preds, pred_dict, u)
        data_dict[(node_type, "to_scan", f"scan")].append((u_node_id, v_node_id))

    # convert intra predicate edges (e.g. column to AND)
    for u, v in intra_predicate_edges:
        u_node_type, u_node_id = pred_node_type_id_func(logical_preds, pred_dict, u)
        v_node_type, v_node_id = pred_node_type_id_func(logical_preds, pred_dict, v)
        data_dict[(u_node_type, "intra_predicate", v_node_type)].append(
            (u_node_id, v_node_id)
        )


def create_node_types_per_depth(plan_depths, join_to_encode_edges, scan_to_join_edges):
    # now create heterograph with node types: table, column, filter_column, logical_pred, output_column, plan{depth}
    # for this, first create mapping of old plan node id -> depth and node id for depth
    plan_dict = dict()
    nodes_per_depth = {0: 0, 1: 0, 2: 0}

    for plan_node, d in enumerate(plan_depths):
        assert d in nodes_per_depth, f"have some depth larger than 2, d"
        plan_dict[plan_node] = nodes_per_depth[d]
        nodes_per_depth[d] += 1
    # create edge and node types depending on depth in the plan
    data_dict = collections.defaultdict(list)
    for u, v in join_to_encode_edges:
        u_node_id = plan_dict[u]
        v_node_id = plan_dict[v]
        data_dict[(f"join", f"to_encode", f"encode")].append((u_node_id, v_node_id))

    for u, v in scan_to_join_edges:
        u_node_id = plan_dict[u]
        v_node_id = plan_dict[v]
        data_dict[(f"scan", f"to_join", f"join")].append((u_node_id, v_node_id))

    return data_dict, nodes_per_depth, plan_dict


def add_numerical_scalers(feature_statistics):
    for k, v in feature_statistics.items():
        if v.get("type") == str(FeatureType.numeric):
            # print("=====================================")
            # print(k, v)
            scaler = RobustScaler()
            scaler.center_ = v["center"]
            scaler.scale_ = v["scale"]
            feature_statistics[k]["scaler"] = scaler


def pred_node_type_id(logical_preds, pred_dict, u):
    if logical_preds[u]:
        u_node_id, depth = pred_dict[u]
        node_type = f"logical_pred_{depth}"
    else:
        u_node_id = pred_dict[u]
        node_type = f"filter_column"
    return node_type, u_node_id
