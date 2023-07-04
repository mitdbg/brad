import collections
import re

from brad.query_stats.plan_parsing.plan_operator import PlanOperator
from brad.query_stats.plan_parsing.postgres_utils import plan_statistics

planning_time_regex = re.compile(r"planning time: (?P<planning_time>\d+.\d+) ms")
ex_time_regex = re.compile(r"execution time: (?P<execution_time>\d+.\d+) ms")
init_plan_regex = re.compile(r"InitPlan \d+ \(returns \$\d\)")
join_columns_regex = re.compile(r"\w+\.\w+ ?= ?\w+\.\w+")


def create_node(lines_plan_operator, operators_current_level):
    if len(lines_plan_operator) > 0:
        last_operator = PlanOperator(lines_plan_operator)
        operators_current_level.append(last_operator)
        lines_plan_operator = []
    return lines_plan_operator


def count_left_whitespaces(a):
    return len(a) - len(a.lstrip(" "))


def parse_recursively(parent, plan, offset, depth):
    lines_plan_operator = []
    i = offset
    operators_current_level = []
    while i < len(plan):
        # new operator
        if plan[i].strip().startswith("->"):
            # create plan node for previous one
            lines_plan_operator = create_node(
                lines_plan_operator, operators_current_level
            )

            # if plan operator is deeper
            new_depth = count_left_whitespaces(plan[i])
            if new_depth > depth:
                assert len(operators_current_level) > 0, "No parent found at this level"
                i = parse_recursively(operators_current_level[-1], plan, i, new_depth)

            # one step up in recursion
            elif new_depth < depth:
                break

            # new operator in current depth
            elif new_depth == depth:
                lines_plan_operator.append(plan[i])
                i += 1

        else:
            lines_plan_operator.append(plan[i])
            i += 1

    create_node(lines_plan_operator, operators_current_level)

    # any node in the recursion
    if parent is not None:
        parent.children = operators_current_level
        return i

    # top node
    else:
        # there should only be one top node
        assert len(operators_current_level) == 1
        return operators_current_level[0]


def parse_plan(analyze_plan_tuples, analyze=True, parse=True):
    plan_steps = analyze_plan_tuples
    if isinstance(analyze_plan_tuples[0], tuple) or isinstance(
        analyze_plan_tuples[0], list
    ):
        plan_steps = [t[0] for t in analyze_plan_tuples]

    # for some reason this is missing in postgres
    # in order to parse this, we add it
    plan_steps[0] = "->  " + plan_steps[0]

    ex_time = 0
    planning_time = 0
    planning_idx = -1
    if analyze:
        for i, plan_step in enumerate(plan_steps):
            plan_step = plan_step.lower()
            ex_time_match = planning_time_regex.match(plan_step)
            if ex_time_match is not None:
                planning_idx = i
                planning_time = float(ex_time_match.groups()[0])

            ex_time_match = ex_time_regex.match(plan_step)
            if ex_time_match is not None:
                ex_time = float(ex_time_match.groups()[0])

        assert ex_time != 0 and planning_time != 0
        plan_steps = plan_steps[:planning_idx]

    root_operator = None
    if parse:
        root_operator = parse_recursively(None, plan_steps, 0, 0)

    return root_operator, ex_time, planning_time


def parse_plans(
    explain_rows,
    min_runtime=100,
    parse_baseline=False,
    parse_join_conds=False,
    zero_card_min_runtime=None,
):
    # keep track of column statistics
    if zero_card_min_runtime is None:
        zero_card_min_runtime = min_runtime
    column_id_mapping = dict()
    table_id_mapping = dict()
    partial_column_name_mapping = collections.defaultdict(set)

    # parse individual queries
    parsed_plans = []
    avg_runtimes = []
    no_tables = []
    no_filters = []
    op_perc = collections.defaultdict(int)
    alias_dict = dict()

    avg_runtime = 0

    # only explain plan (not executed)
    verbose_plan, _, _ = parse_plan(explain_rows, analyze=False, parse=True)
    verbose_plan.parse_lines_recursively(
        alias_dict=alias_dict,
        parse_baseline=parse_baseline,
        parse_join_conds=parse_join_conds,
    )

    analyze_plan = verbose_plan
    tables, filter_columns, _ = plan_statistics(analyze_plan)

    analyze_plan.parse_columns_bottom_up(
        column_id_mapping,
        partial_column_name_mapping,
        table_id_mapping,
        alias_dict=alias_dict,
    )
    analyze_plan.tables = tables
    analyze_plan.num_tables = len(tables)
    analyze_plan.plan_runtime = avg_runtime

    def augment_no_workers(p, top_no_workers=0):
        no_workers = p.plan_parameters.get("workers_planned")
        if no_workers is None:
            no_workers = top_no_workers

        p.plan_parameters["workers_planned"] = top_no_workers

        for c in p.children:
            augment_no_workers(c, top_no_workers=no_workers)

    augment_no_workers(analyze_plan)

    # collect statistics
    avg_runtimes.append(avg_runtime)
    no_tables.append(len(tables))
    for _, op in filter_columns:
        op_perc[op] += 1
    # log number of filters without counting AND, OR
    no_filters.append(len([fc for fc in filter_columns if fc[0] is not None]))

    if "tables" in analyze_plan:
        analyze_plan["tables"] = list(analyze_plan["tables"])
    else:
        analyze_plan["tables"] = []

    parsed_plans.append(analyze_plan)

    parsed_runs = dict(parsed_plans=parsed_plans)
    stats = dict(
        runtimes=str(avg_runtimes), no_tables=str(no_tables), no_filters=str(no_filters)
    )

    return parsed_runs, stats


def normalize_join_condition(p_join_str):
    join_conds = p_join_str.split("AND")
    join_conds = [normalize_single_join_condition(jc.strip()) for jc in join_conds]
    join_conds = sorted(join_conds)
    join_conds = " AND ".join(join_conds)
    return join_conds


def normalize_single_join_condition(p_join_str):
    join_cond = p_join_str.split("=")
    assert len(join_cond) == 2
    for i in [0, 1]:
        join_cond[i] = join_cond[i].strip()
    join_cond = sorted(join_cond)
    join_cond = f"{join_cond[0]} = {join_cond[1]}"
    return join_cond
