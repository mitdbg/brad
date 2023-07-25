class RedshiftTrueCardDetail:
    ENCODE_FEATURES = ["num_tables", "num_joins"]
    SCAN_FEATURES = ["act_card", "est_width", "act_children_card"]
    # JOIN_FEATURES should add join type (etc inner, outer), we should also add memory size and cpu.
    JOIN_FEATURES = ["act_card", "est_width", "act_children_card"]
    FILTER_FEATURES = ["operator", "literal_feature"]
    # JOIN_FEATURES should add num_of_pages for columns and their encoding type.
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relcols"]


class RedshiftEstSystemCardDetail:
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
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relcols"]


class RedshiftCardCorrectorDetail:
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
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    TABLE_FEATURES = ["reltuples", "relcols"]
