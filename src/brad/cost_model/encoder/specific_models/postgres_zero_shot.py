# We adapted the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
from cost_model.dataset.plan_featurization import postgres_plan_featurizations
from cost_model.dataset.query_featurization import postgres_query_featurizations
from cost_model.encoder.zero_shot_model import ZeroShotModel


class PostgresZeroShotModelPlan(ZeroShotModel):
    """
    Zero-shot cost estimation model for postgres.
    """

    def __init__(self, plan_featurization_name=None, **zero_shot_kwargs):
        plan_featurization, encoders = None, None
        self.plan_featurization_name = plan_featurization_name
        if plan_featurization_name is not None:
            plan_featurization = postgres_plan_featurizations.__dict__[
                plan_featurization_name
            ]

            # define the MLPs for the different node types in the graph representation of queries
            encoders = [
                ("column", plan_featurization.COLUMN_FEATURES),
                ("table", plan_featurization.TABLE_FEATURES),
                ("output_column", plan_featurization.OUTPUT_COLUMN_FEATURES),
                (
                    "filter_column",
                    plan_featurization.FILTER_FEATURES
                    + plan_featurization.COLUMN_FEATURES,
                ),
                ("plan", plan_featurization.PLAN_FEATURES),
                ("logical_pred", plan_featurization.FILTER_FEATURES),
            ]

        # define messages passing which is peculiar for postgres
        prepasses = [dict(model_name="column_output_column", e_name="col_output_col")]
        tree_model_types = [
            "column_output_column",
            "to_plan",
            "intra_plan",
            "intra_pred",
        ]

        super().__init__(
            plan_featurization=plan_featurization,
            encoders=encoders,
            prepasses=prepasses,
            add_tree_model_types=tree_model_types,
            **zero_shot_kwargs
        )


class PostgresZeroShotModel(ZeroShotModel):
    """
    Zero-shot cost estimation model for postgres.
    """

    def __init__(self, query_featurization_name=None, **zero_shot_kwargs):
        query_featurization, encoders = None, None
        self.plan_featurization_name = query_featurization_name
        if query_featurization_name is not None:
            query_featurization = postgres_query_featurizations.__dict__[
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
            **zero_shot_kwargs
        )
