from typing import Any, Dict, List, Tuple

from .parse_plan import parse_plans


def parse_explain_verbose(lines: List[str] | List[Tuple[str]]) -> Dict[str, Any]:
    """
    Returns a parsed representation of PostgreSQL's EXPLAIN VERBOSE.

    The input to this function should be a list of the outputted rows from
    executing EXPLAIN VERBOSE.
    """
    results, _ = parse_plans(lines)
    return results["parsed_plans"][0]
