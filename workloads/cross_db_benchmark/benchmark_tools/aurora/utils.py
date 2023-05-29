import numpy as np
import re


def _CanonicalizeJoinCond(join_cond, is_brad=False):
    """join_cond: 4-tuple"""
    t1, c1, t2, c2 = join_cond
    if is_brad and not t1.endswith("_brad_source"):
        t1 = t1 + "_brad_source"
    if is_brad and not t2.endswith("_brad_source"):
        t2 = t2 + "_brad_source"
    if t1 < t2:
        return t1, c1, t2, c2
    return t2, c2, t1, c1


def _DedupJoinConds(join_conds, is_brad=False):
    """join_conds: list of 4-tuple (t1, c1, t2, c2)."""
    canonical_join_conds = [_CanonicalizeJoinCond(jc, is_brad) for jc in join_conds]
    return sorted(set(canonical_join_conds))


def _GetJoinConds(sql, is_brad=False):
    """Returns a list of join conditions in the form of (t1, c1, t2, c2)."""
    join_cond_pat = re.compile(
        r"""
        (\w+)  # 1st table
        \.     # the dot "."
        (\w+)  # 1st table column
        \s*    # optional whitespace
        =      # the equal sign "="
        \s*    # optional whitespace
        (\w+)  # 2nd table
        \.     # the dot "."
        (\w+)  # 2nd table column
        """,
        re.VERBOSE,
    )
    join_conds = join_cond_pat.findall(sql)
    if len(join_conds) == 0:
        join_cond_pat = re.compile(
            r"""
            \"
            (\w+)  # 1st table
            \"
            \.     # the dot "."
            \"
            (\w+)  # 1st table column
            \"
            \s*    # optional whitespace
            =      # the equal sign "="
            \s*    # optional whitespace
            \"
            (\w+)  # 2nd table
            \"
            \.     # the dot "."
            \"
            (\w+)  # 2nd table column
            \"
            """,
            re.VERBOSE,
        )
        join_conds = join_cond_pat.findall(sql)
        return _DedupJoinConds(join_conds, is_brad)
    return _DedupJoinConds(join_conds, is_brad)


def _FormatJoinCond(tup):
    t1, c1, t2, c2 = tup
    return f"{t1}.{c1} = {t2}.{c2}"


def plan_statistics(
    plan_op,
    tables=None,
    filter_columns=None,
    operators=None,
    skip_columns=False,
    conv_to_dict=False,
):
    if tables is None:
        tables = set()
    if operators is None:
        operators = set()
    if filter_columns is None:
        filter_columns = set()

    params = plan_op.plan_parameters

    if conv_to_dict:
        params = vars(params)

    if "table" in params:
        tables.add(params["table"])
    if "op_name" in params:
        operators.add(params["op_name"])
    if "filter_columns" in params and not skip_columns:
        list_columns(params["filter_columns"], filter_columns)

    for c in plan_op.children:
        plan_statistics(
            c,
            tables=tables,
            filter_columns=filter_columns,
            operators=operators,
            skip_columns=skip_columns,
            conv_to_dict=conv_to_dict,
        )

    return tables, filter_columns, operators


def child_prod(p, feature_name, default=1):
    child_feat = [
        c.plan_parameters.get(feature_name)
        for c in p.children
        if c.plan_parameters.get(feature_name) is not None
    ]
    if len(child_feat) == 0:
        return default
    return np.prod(child_feat)


def list_columns(n, columns):
    columns.add((n.column, n.operator))
    for c in n.children:
        list_columns(c, columns)


def get_leaf_nodes(root, leaves):
    if "children" not in root or len(root["children"]) == 0:
        leaves.append(root["plan_parameters"])
    else:
        for child in root["children"]:
            get_leaf_nodes(child, leaves)


def getJoinConds(
    alias_dict, sql, table_id_mapping=None, column_id_mapping=None, is_brad=False
):
    joins = _GetJoinConds(sql, is_brad)
    join_conds = dict()
    t1_alias = None
    t2_alias = None
    for (t1, k1, t2, k2) in joins:
        clause = _FormatJoinCond((t1, k1, t2, k2))
        if alias_dict is not None:
            if t1 in alias_dict:
                t1_alias = t1
                t1 = alias_dict[t1]
            if t2 in alias_dict:
                t2_alias = t2
                t2 = alias_dict[t2]
        if table_id_mapping and t1 in table_id_mapping and t2 in table_id_mapping:
            t1_id = table_id_mapping[t1]
            t2_id = table_id_mapping[t2]
        else:
            t1_id = t1
            t2_id = t2
        if (
            column_id_mapping
            and (t1, k1) in column_id_mapping
            and (t2, k2) in column_id_mapping
        ):
            k1_id = column_id_mapping[(t1, k1)]
            k2_id = column_id_mapping[(t2, k2)]
        else:
            k1_id = k1
            k2_id = k2
        join_conds[clause] = ([t1_id, k1_id, t2_id, k2_id], [t1_alias, t2_alias])
    return join_conds


def getFilters(plan, return_text=False):
    leaf_nodes = []
    get_leaf_nodes(plan, leaf_nodes)
    filter_texts = dict()
    filter_nodes = dict()
    for leaf in leaf_nodes:
        if "table" in leaf:
            # this is a scan node, maybe this if statement is not needed
            if "alias" in leaf and leaf["alias"] is not None:
                scan_id = leaf["alias"]
            else:
                scan_id = leaf["table"]
            if return_text:
                filter_text = None
                if (
                    "filter_text" in leaf
                    and leaf["filter_text"] is not None
                    and len(leaf["filter_text"]) != 0
                ):
                    filter_text = leaf["filter_text"]
                filter_texts[scan_id] = filter_text
            filter_nodes[scan_id] = leaf
    return filter_nodes, filter_texts
