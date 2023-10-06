from collections import namedtuple
from typing import Any, Dict, List, Tuple, Optional

from .parse_plan import parse_plans

ParsedPlan = Dict[str, Any]
BaseCardinality = namedtuple(
    "BaseCardinality", ["table_name", "cardinality", "width", "access_op_name"]
)


def parse_explain_verbose(lines: List[str] | List[Tuple[Any, ...]]) -> ParsedPlan:
    """
    Returns a parsed representation of PostgreSQL's EXPLAIN VERBOSE. This method
    also works for Redshift plans (EXPLAIN only) but does not include as much
    detail.

    The input to this function should be a list of the outputted rows from
    executing EXPLAIN VERBOSE (PostgreSQL) or EXPLAIN (Redshift).
    """
    results, _ = parse_plans(lines)
    return results["parsed_plans"][0]


def extract_base_cardinalities(plan: ParsedPlan) -> List[BaseCardinality]:
    """
    Extracts the number of rows accessed from each base table, based on the plan
    returned by EXPLAIN.
    """
    base_cardinalities = []
    ops: List[Tuple[ParsedPlan, Optional[ParsedPlan], int]] = [(plan, None, 0)]
    while len(ops) > 0:
        op, parent, visit_count = ops.pop()
        if visit_count != 0:
            if (
                "child_is_index_scan" in op
                and "Scan" in op["plan_parameters"]["op_name"]
            ):
                # This is a somewhat hacky way to handle the case of a bitmap
                # scan executed using an index (which is two operators, with the
                # index scan being the base op). This logic will only work where
                # the index scan is a direct descendant.
                base_cardinalities.append(
                    BaseCardinality(
                        op["plan_parameters"]["table"],
                        op["plan_parameters"]["est_card"],
                        op["plan_parameters"]["est_width"],
                        op["plan_parameters"]["op_name"],
                    )
                )

            # Regardless, stop processing this op.
            continue

        if len(op["children"]) == 0:
            if op["plan_parameters"]["table"].endswith("_index") and parent is not None:
                # This is a straight scan of the index. We do not get table
                # cardinality information from this operator.
                parent["child_is_index_scan"] = True
                continue

            # This is a base operator.
            base_cardinalities.append(
                BaseCardinality(
                    op["plan_parameters"]["table"],
                    op["plan_parameters"]["est_card"],
                    op["plan_parameters"]["est_width"],
                    op["plan_parameters"]["op_name"],
                )
            )
        else:
            ops.append((op, parent, 1))
            for child in op["children"]:
                # node, parent, visit count
                ops.append((child, op, 0))
    return base_cardinalities
