class PostgresTrueCardDetail:
    ENCODE_FEATURES = ["num_tables", "num_joins"]
    SCAN_FEATURES = ["act_card", "est_width", "act_children_card"]
    # JOIN_FEATURES should and join type (etc inner, outer), we should also add memory size if possible.
    JOIN_FEATURES = ["act_card", "est_width", "act_children_card"]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
        "has_index",
        "is_pk",
        "is_fk",
        "is_sorted",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages", "relcols"]


class PostgresEstSystemCardDetail:
    ENCODE_FEATURES = ["num_tables", "num_joins"]
    SCAN_FEATURES = ["est_card", "est_width", "est_children_card"]
    JOIN_FEATURES = ["est_card", "est_width", "est_children_card"]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
        "has_index",
        "is_pk",
        "is_fk",
        "is_sorted",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages", "relcols"]


class PostgresCardCorrectorDetail:
    ENCODE_FEATURES = ["num_tables", "num_joins"]
    SCAN_FEATURES = ["cc_est_card", "est_width", "cc_est_children_card"]
    JOIN_FEATURES = ["cc_est_card", "est_width", "cc_est_children_card"]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
        "has_index",
        "is_pk",
        "is_fk",
        "is_sorted",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relpages", "relcols"]
