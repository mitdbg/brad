class AthenaTrueCardDetail:
    ENCODE_FEATURES = ["num_tables", "num_joins"]
    SCAN_FEATURES = ["act_card", "est_width", "act_children_card"]
    # JOIN_FEATURES should add join type (etc inner, outer), we should also add memory size and cpu.
    JOIN_FEATURES = ["act_card", "est_width", "act_children_card"]
    FILTER_FEATURES = ["operator", "literal_feature"]
    COLUMN_FEATURES = [
        "avg_width",
        "correlation",
        "data_type",
        "n_distinct",
        "null_frac",
    ]
    OUTPUT_COLUMN_FEATURES = ["aggregation"]
    # Should add table size in GB
    TABLE_FEATURES = ["reltuples", "relcols"]


class AthenaEstSystemCardDetail:
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


class AthenaCardCorrectorDetail:
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