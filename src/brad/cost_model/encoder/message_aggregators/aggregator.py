# Legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
import torch

from cost_model.encoder.utils.fc_out_model import FcOutModel


class MessageAggregator(FcOutModel):
    """
    Abstract message aggregator class. Combines child messages (either using MSCN or GAT) and afterwards combines the
    hidden state with the aggregated messages using an MLP.
    """

    def __init__(self, test=False, **fc_out_kwargs):
        super().__init__(**fc_out_kwargs)
        self.test = test

    def forward(
        self,
        graph=None,
        etypes=None,
        out_node_types=None,
        feat_dict=None,
        return_feature=False,
    ):
        raise NotImplementedError

    def combine(self, feat, out_node_types, rst, return_feature=False):
        out_dict = dict()
        return_feat = None
        for out_type in out_node_types:
            if out_type in feat and out_type in rst:
                assert feat[out_type].shape[0] == rst[out_type].shape[0]
                # send through fully connected layers
                if return_feature:
                    return_feat = torch.clone(rst[out_type])
                if not self.test:
                    out_dict[out_type] = self.model(
                        torch.cat([feat[out_type], rst[out_type]], dim=1)
                    )
                # simply add in debug mode for testing
                else:
                    out_dict[out_type] = feat[out_type] + rst[out_type]
        if return_feature:
            return out_dict, return_feat
        else:
            return out_dict
