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
