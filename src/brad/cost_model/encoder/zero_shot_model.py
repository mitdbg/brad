# We adapted the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation

import torch
from torch import nn
import numpy as np

from brad.cost_model.encoder.message_aggregators import message_aggregators
from brad.cost_model.encoder.utils.fc_out_model import FcOutModel
from brad.cost_model.encoder.utils.node_type_encoder import NodeTypeEncoder


class ZeroShotModel(nn.Module):
    """
    A zero-shot cost model that predicts query runtimes on unseen databases out-of-the-box without retraining.
    """

    def __init__(
        self,
        device="cpu",
        hidden_dim=None,
        final_mlp_kwargs=None,
        output_dim=1,
        tree_layer_name=None,
        tree_layer_kwargs=None,
        test=False,
        FC_test_layer=None,
        skip_message_passing=False,
        node_type_kwargs=None,
        feature_statistics=None,
        add_tree_model_types=None,
        prepasses=None,
        plan_featurization=None,
        encoders=None,
        label_norm=None,
        return_feat=False,
        table_name_map=None,
    ):
        super().__init__()
        # super().__init__(output_dim=output_dim, input_dim=hidden_dim, final_out_layer=True, **final_mlp_kwargs)

        self.label_norm = label_norm

        self.test = test
        self.FC_test_layer = FC_test_layer
        self.skip_message_passing = skip_message_passing
        self.device = device
        self.hidden_dim = hidden_dim
        self.return_feat = return_feat

        # use different models per edge type
        self.skip_message_passing = skip_message_passing
        self.device = device
        self.hidden_dim = hidden_dim

        # use different models per edge type
        tree_model_types = add_tree_model_types
        self.tree_models = nn.ModuleDict(
            {
                node_type: message_aggregators.__dict__[tree_layer_name](
                    hidden_dim=self.hidden_dim, **tree_layer_kwargs
                )
                for node_type in tree_model_types
            }
        )

        # these message passing steps are performed in the beginning (dependent on the concrete database system at hand)
        self.prepasses = prepasses

        if plan_featurization is not None:
            self.plan_featurization = plan_featurization
            # different models to encode plans, tables, columns, filter_columns and output_columns
            node_type_kwargs.update(output_dim=hidden_dim)
            self.node_type_encoders = nn.ModuleDict(
                {
                    enc_name: NodeTypeEncoder(
                        features, feature_statistics, **node_type_kwargs
                    )
                    for enc_name, features in encoders
                }
            )
        self.fcout = FcOutModel(
            output_dim=output_dim,
            input_dim=hidden_dim,
            final_out_layer=True,
            **final_mlp_kwargs,
        )
        self.loss_fxn = self.fcout.loss_fxn
        self.table_name_map = table_name_map

    def encode_node_types(self, g, features, return_raw_feature=False):
        """
        Initializes the hidden states based on the node type specific models.
        """
        # initialize hidden state per node type
        hidden_dict = dict()
        raw_scan_feature = None
        raw_join_feature = None
        for node_type, input_features in features.items():
            # encode all plans with same model
            if return_raw_feature:
                if node_type == "scan":
                    raw_scan_feature = torch.clone(input_features)
                if node_type == "join":
                    raw_join_feature = torch.clone(input_features)
            if node_type not in self.node_type_encoders.keys():
                assert node_type.startswith("logical_pred")
                node_type_m = self.node_type_encoders["logical_pred"]
            else:
                node_type_m = self.node_type_encoders[node_type]
            hidden_dict[node_type] = node_type_m(input_features)

        if return_raw_feature:
            return hidden_dict, raw_scan_feature, raw_join_feature
        else:
            return hidden_dict

    def forward(self, input, inplace=False):
        """
        Returns logits for output classes
        """
        graph, features = input
        features = self.encode_node_types(graph, features)
        out = self.message_passing(graph, features, inplace)

        return out

    def message_passing(self, g, feat_dict, inplace=False):
        """
        Bottom-up message passing on the graph encoding of the queries in the batch. Returns the hidden states of the
        root nodes.
        """

        # also allow skipping this for testing
        if not self.skip_message_passing:
            # all passes before predicates, to plan and intra_plan passes
            pass_directions = [
                PassDirection(g=g, **prepass_kwargs)
                for prepass_kwargs in self.prepasses
            ]

            if g.max_pred_depth is not None:
                # intra_pred from deepest node to top node
                for d in reversed(range(g.max_pred_depth)):
                    pd = PassDirection(
                        model_name="intra_pred",
                        g=g,
                        e_name="intra_predicate",
                        n_dest=f"logical_pred_{d}",
                    )
                    pass_directions.append(pd)

            # filter_columns & output_columns to plan
            pass_directions.append(
                PassDirection(model_name="to_scan", g=g, e_name="to_scan")
            )
            pass_directions.append(
                PassDirection(model_name="to_join", g=g, e_name="to_join")
            )
            pass_directions.append(
                PassDirection(model_name="to_encode", g=g, e_name="to_encode")
            )

            # make sure all edge types are considered in the message passing
            combined_e_types = set()
            for pd in pass_directions:
                combined_e_types.update(pd.etypes)
            assert combined_e_types == set(g.canonical_etypes)

            for pd in pass_directions:
                if len(pd.etypes) > 0:
                    out_dict = self.tree_models[pd.model_name](
                        g,
                        etypes=pd.etypes,
                        in_node_types=pd.in_types,
                        out_node_types=pd.out_types,
                        feat_dict=feat_dict,
                    )
                    for out_type, hidden_out in out_dict.items():
                        feat_dict[out_type] = hidden_out

        # compute top nodes of dags
        out = feat_dict["encode"]

        # feed them into final feed forward network
        if not self.test:
            out = self.fcout(out)
        elif self.FC_test_layer is not None:
            out = self.fcout(out, self.FC_test_layer, inplace)
        return out

    def featurize(self, input, sample_idx_map, inplace=False, with_quote=False):
        graph, features = input
        feat_dict, scan_feature, join_feature = self.encode_node_types(
            graph, features, return_raw_feature=True
        )
        assert scan_feature is not None and join_feature is not None
        pass_directions = [
            PassDirection(g=graph, **prepass_kwargs)
            for prepass_kwargs in self.prepasses
        ]

        if graph.max_pred_depth is not None:
            # intra_pred from deepest node to top node
            for d in reversed(range(graph.max_pred_depth)):
                pd = PassDirection(
                    model_name="intra_pred",
                    g=graph,
                    e_name="intra_predicate",
                    n_dest=f"logical_pred_{d}",
                )
                pass_directions.append(pd)

        # filter_columns & output_columns to plan
        pass_directions.append(
            PassDirection(model_name="to_scan", g=graph, e_name="to_scan")
        )
        pass_directions.append(
            PassDirection(model_name="to_join", g=graph, e_name="to_join")
        )
        pass_directions.append(
            PassDirection(model_name="to_encode", g=graph, e_name="to_encode")
        )

        # make sure all edge types are considered in the message passing
        combined_e_types = set()
        for pd in pass_directions:
            combined_e_types.update(pd.etypes)
        assert combined_e_types == set(graph.canonical_etypes)
        scan_children_feature = None

        for pd in pass_directions:
            if len(pd.etypes) > 0:
                if pd.model_name == "to_scan":
                    assert len(pd.out_types) == 1, pd.out_types
                    out_dict, scan_children_feature = self.tree_models[pd.model_name](
                        graph,
                        etypes=pd.etypes,
                        in_node_types=pd.in_types,
                        out_node_types=pd.out_types,
                        feat_dict=feat_dict,
                        return_feature=True,
                    )
                elif pd.model_name == "to_join":
                    assert len(pd.out_types) == 1, pd.out_types
                    out_dict = self.tree_models[pd.model_name](
                        graph,
                        etypes=pd.etypes,
                        in_node_types=pd.in_types,
                        out_node_types=pd.out_types,
                        feat_dict=feat_dict,
                        return_feature=False,
                    )
                else:
                    out_dict = self.tree_models[pd.model_name](
                        graph,
                        etypes=pd.etypes,
                        in_node_types=pd.in_types,
                        out_node_types=pd.out_types,
                        feat_dict=feat_dict,
                    )
                for out_type, hidden_out in out_dict.items():
                    feat_dict[out_type] = hidden_out

        out = feat_dict["encode"]
        filter_feat = feat_dict["filter_column"]
        if self.FC_test_layer is not None:
            out = self.fcout(out, self.FC_test_layer, inplace)

        assert scan_children_feature is not None
        scan_feature = scan_feature.detach().numpy()
        scan_children_feature = scan_children_feature.detach().numpy()
        join_feature = join_feature.detach().numpy()
        filter_feat = filter_feat.detach().numpy()
        scan_feature_dict = dict()
        join_feature_dict = dict()
        query_feature_dict = dict()
        for i, query_idx in enumerate(sample_idx_map):
            query_feature_dict[query_idx] = out.detach().numpy()[i]
            scan_feature_dict[query_idx] = dict()
            join_feature_dict[query_idx] = dict()
            for table in sample_idx_map[query_idx]["scan"]:
                scan_node_idx = sample_idx_map[query_idx]["scan"][table]
                curr_table_id = None
                if type(scan_node_idx) == tuple:
                    scan_node_idx, curr_table_id = scan_node_idx
                if self.table_name_map is not None:
                    if curr_table_id is None:
                        curr_table_id = int(table)
                    table_name = self.table_name_map[curr_table_id]
                    if with_quote:
                        table_name = '"' + table_name + '"'
                    scan_feature_dict[query_idx][table_name] = np.concatenate(
                        (
                            scan_feature[scan_node_idx],
                            scan_children_feature[scan_node_idx],
                        )
                    )
                else:
                    if with_quote:
                        table_name = '"' + table + '"'
                    else:
                        table_name = table
                    scan_feature_dict[query_idx][table_name] = np.concatenate(
                        (
                            scan_feature[scan_node_idx],
                            scan_children_feature[scan_node_idx],
                        )
                    )
            for tables in sample_idx_map[query_idx]["join"]:
                join_node_idx = sample_idx_map[query_idx]["join"][tables]
                jfp_node_idx = sample_idx_map[query_idx]["join_predicate"][
                    tables
                ]  # join filter predicate
                # Small hack: we have each join containing two filter columns (join keys)
                jfp_feature = filter_feat[jfp_node_idx] + filter_feat[jfp_node_idx + 1]
                if self.table_name_map is not None:
                    names = (
                        self.table_name_map[tables[0]],
                        self.table_name_map[tables[1]],
                    )
                    if with_quote:
                        names = ('"' + names[0] + '"', '"' + names[1] + '"')
                    join_feature_dict[query_idx][tuple(sorted(names))] = np.concatenate(
                        (join_feature[join_node_idx], jfp_feature)
                    )
                else:
                    names = tuple(sorted(tables))
                    if with_quote:
                        names = ('"' + names[0] + '"', '"' + names[1] + '"')
                    join_feature_dict[query_idx][names] = np.concatenate(
                        (join_feature[join_node_idx], jfp_feature)
                    )

        return query_feature_dict, scan_feature_dict, join_feature_dict


class PassDirection:
    """
    Defines a message passing step on the encoded query graphs.
    """

    def __init__(self, model_name, g, e_name=None, n_dest=None, allow_empty=False):
        """
        Initializes a message passing step.
        :param model_name: which edge model should be used to combine the messages
        :param g: the graph on which the message passing should be performed
        :param e_name: edges are defined by triplets: (src_node_type, edge_type, dest_node_type). Only incorporate edges
            in the message passing step where edge_type=e_name
        :param n_dest: further restrict the edges that are incorporated in the message passing by the condition
            dest_node_type=n_dest
        :param allow_empty: allow that no edges in the graph qualify for this message passing step. Otherwise this will
            raise an error.
        """
        self.etypes = set()
        self.in_types = set()
        self.out_types = set()
        self.model_name = model_name

        for curr_n_src, curr_e_name, curr_n_dest in g.canonical_etypes:
            if e_name is not None and curr_e_name != e_name:
                continue

            if n_dest is not None and curr_n_dest != n_dest:
                continue

            self.etypes.add((curr_n_src, curr_e_name, curr_n_dest))
            self.in_types.add(curr_n_src)
            self.out_types.add(curr_n_dest)

        self.etypes = list(self.etypes)
        self.in_types = list(self.in_types)
        self.out_types = list(self.out_types)
        if not allow_empty:
            assert (
                len(self.etypes) > 0
            ), f"No nodes in the graph qualify for e_name={e_name}, n_dest={n_dest}"
