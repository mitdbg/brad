# Legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
class PostgresTrueCardDetail:
    PLAN_FEATURES = [
        "act_card",
        "est_width",
        "workers_planned",
        "op_name",
        "act_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]


class PostgresEstSystemCardDetail:
    PLAN_FEATURES = [
        "est_card",
        "est_width",
        "workers_planned",
        "op_name",
        "est_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]


class PostgresTunedCardDetail:
    PLAN_FEATURES = [
        "tuned_est_card",
        "est_width",
        "workers_planned",
        "op_name",
        "tuned_est_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]


class PostgresDeepDBEstSystemCardDetail:
    PLAN_FEATURES = [
        "dd_est_card",
        "est_width",
        "workers_planned",
        "op_name",
        "dd_est_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]


class PostgresCardCorrectorDetail:
    PLAN_FEATURES = [
        "cc_est_card",
        "est_width",
        "workers_planned",
        "op_name",
        "cc_est_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]


class AuroraTrueCardDetail:
    PLAN_FEATURES = [
        "act_card",
        "est_width",
        "workers_planned",
        "op_name",
        "act_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]


class AuroraEstSystemCardDetail:
    PLAN_FEATURES = [
        "est_card",
        "est_width",
        "workers_planned",
        "op_name",
        "est_children_card",
    ]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages"]
