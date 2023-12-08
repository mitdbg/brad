import numpy as np
from sklearn.linear_model import LinearRegression

from brad.cost_model.dataset.query_featurization import athena_query_featurization
from brad.cost_model.encoder.zero_shot_model import ZeroShotModel
from workloads.cross_db_benchmark.benchmark_tools.aurora.utils import _GetJoinConds


class AthenaZeroShotModel(ZeroShotModel):
    """
    Zero-shot cost estimation model for postgres.
    """

    def __init__(self, query_featurization_name=None, **zero_shot_kwargs):
        query_featurization, encoders = None, None
        self.plan_featurization_name = query_featurization_name
        if query_featurization_name is not None:
            query_featurization = athena_query_featurization.__dict__[
                query_featurization_name
            ]

            # define the MLPs for the different node types in the graph representation of queries
            encoders = [
                ("encode", query_featurization.ENCODE_FEATURES),
                ("column", query_featurization.COLUMN_FEATURES),
                ("table", query_featurization.TABLE_FEATURES),
                ("output_column", query_featurization.OUTPUT_COLUMN_FEATURES),
                (
                    "filter_column",
                    query_featurization.FILTER_FEATURES
                    + query_featurization.COLUMN_FEATURES,
                ),
                ("join", query_featurization.SCAN_FEATURES),
                ("scan", query_featurization.JOIN_FEATURES),
                ("logical_pred", query_featurization.FILTER_FEATURES),
            ]

        # define messages passing which is peculiar for postgres
        prepasses = [dict(model_name="column_output_column", e_name="col_output_col")]
        # define the edge type
        tree_model_types = [
            "column_output_column",
            "intra_pred",
            "to_scan",
            "to_join",
            "to_encode",
        ]

        super().__init__(
            plan_featurization=query_featurization,
            encoders=encoders,
            prepasses=prepasses,
            add_tree_model_types=tree_model_types,
            **zero_shot_kwargs,
        )


class AthenaNativeCostModel:
    def __init__(self, slope=1.0, intercept=0, model=None):
        self.slope = slope
        self.intercept = intercept
        self.model = model

    def parse_raw(
        self,
        raw,
        min_query_runtime=100,
        test_query=None,
        min_num_tables=None,
        max_num_tables=None,
    ):
        feature = []
        label = []
        test_idx = []

        for q in raw["query_list"]:
            if (
                q["status"] != "SUCCEEDED"
                or q["runtime_stats"] is None
                or len(q["runtime_stats"]) == 0
            ):
                continue
            runtime = q["runtime_stats"]["Timeline"]["TotalExecutionTimeInMillis"]
            if runtime > min_query_runtime:
                label.append(runtime)
            else:
                continue

            est_cost = q["runtime_stats"]["Rows"]["InputBytes"] / 1024 / 1024
            feature.append(est_cost)

            if test_query is not None:
                sql = q["exec_info"]["Query"] + ";"
                if sql in test_query:
                    test_idx.append(len(label) - 1)
            elif min_num_tables is not None or max_num_tables is not None:
                if min_num_tables is None:
                    min_num_tables = 0
                if max_num_tables is None:
                    max_num_tables = 1000
                sql = q["exec_info"]["Query"] + ";"
                num_tables = len(_GetJoinConds(sql)) + 1
                if min_num_tables <= num_tables <= max_num_tables:
                    test_idx.append(len(label) - 1)

        train_feature = [feature[i] for i in range(len(feature)) if i not in test_idx]
        test_feature = [feature[i] for i in range(len(feature)) if i in test_idx]
        train_label = [label[i] for i in range(len(label)) if i not in test_idx]
        test_label = [label[i] for i in range(len(label)) if i in test_idx]
        return train_feature, train_label, test_feature, test_label

    def evaluate(self, feature, label):
        pred = self.model.predict(feature)
        pred = np.abs(pred)
        qerror = np.maximum(label / pred, pred / label)
        for i in [50, 95, 99]:
            print(f"{i} percentile is {np.percentile(qerror, i)}")
        return pred / 1000

    def train_and_test(
        self,
        raw,
        test_raw=None,
        min_query_runtime=100,
        test_query=None,
        min_num_tables=None,
        max_num_tables=None,
    ):
        train_feature, train_label, test_feature, test_label = self.parse_raw(
            raw, min_query_runtime, test_query, min_num_tables, max_num_tables
        )
        if test_raw is not None:
            test_feature, test_label, _, _ = self.parse_raw(
                test_raw, min_query_runtime, test_query, min_num_tables, max_num_tables
            )
        train_feature = np.asarray(train_feature).reshape(-1, 1)
        train_label = np.asarray(train_label)
        model = LinearRegression()
        model.fit(train_feature, train_label)
        self.model = model
        self.slope = model.coef_
        self.intercept = model.intercept_

        test_feature = np.asarray(test_feature).reshape(-1, 1)
        test_label = np.asarray(test_label)
        pred = self.evaluate(test_feature, test_label)
        return pred
