from collections import namedtuple
from typing import Any, Dict, List, Tuple

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
    ops = [plan]
    while len(ops) > 0:
        op = ops.pop()
        if len(op["children"]) == 0:
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
            for child in op["children"]:
                ops.append(child)
    return base_cardinalities
