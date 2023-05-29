# We modified the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
import collections
import os
from enum import Enum

import numpy as np

from workloads.cross_db_benchmark.benchmark_tools.column_types import Datatype
from workloads.cross_db_benchmark.benchmark_tools.utils import (
    load_schema_json,
    load_column_statistics,
    load_string_statistics,
)


class Operator(Enum):
    NEQ = "!="
    EQ = "="
    LEQ = "<="
    GEQ = ">="
    LIKE = "LIKE"
    NOT_LIKE = "NOT LIKE"
    IS_NOT_NULL = "IS NOT NULL"
    IS_NULL = "IS NULL"
    IN = "IN"
    BETWEEN = "BETWEEN"

    def __str__(self):
        return self.value


class Aggregator(Enum):
    AVG = "AVG"
    SUM = "SUM"
    COUNT = "COUNT"

    def __str__(self):
        return self.value


class ExtendedAggregator(Enum):
    MIN = "MIN"
    MAX = "MAX"

    def __str__(self):
        return self.value


def sample_acyclic_aggregation_query(
    column_stats,
    string_stats,
    group_by_threshold,
    int_neq_predicate_threshold,
    max_cols_per_agg,
    max_no_aggregates,
    max_no_group_by,
    max_no_joins,
    min_no_joins,
    max_no_predicates,
    min_no_predicates,
    relationships_table,
    schema,
    randstate,
    complex_predicates,
    max_no_joins_static,
    max_no_aggregates_static,
    max_no_predicates_static,
    max_no_group_by_static,
    left_outer_join_ratio,
    groupby_limit_prob,
    groupby_having_prob,
    full_outer_join=False,
):
    no_joins = randstate.randint(min_no_predicates, max_no_joins + 1)
    no_predicates = randstate.randint(min_no_predicates + 1, max_no_predicates + 1)
    no_aggregates = randstate.randint(1, max_no_aggregates + 1)
    no_group_bys = randstate.randint(0, max_no_group_by + 1)

    if max_no_joins_static:
        no_joins = max_no_joins
    if max_no_predicates_static:
        no_predicates = max_no_predicates
    if max_no_aggregates_static:
        no_aggregates = max_no_aggregates
    if max_no_group_by_static:
        no_group_bys = max_no_group_by

    start_t, joins, join_tables = sample_acyclic_join(
        no_joins, relationships_table, schema, randstate, left_outer_join_ratio
    )

    (
        numerical_aggregation_columns,
        possible_group_by_columns,
        predicates,
    ) = generate_predicates(
        column_stats,
        complex_predicates,
        group_by_threshold,
        int_neq_predicate_threshold,
        join_tables,
        no_predicates,
        randstate,
        string_stats,
    )
    limit = None
    if randstate.rand() < groupby_limit_prob:
        limit = randstate.choice([10, 100, 1000])

    group_bys = sample_group_bys(no_group_bys, possible_group_by_columns, randstate)
    aggregations = sample_aggregations(
        max_cols_per_agg,
        no_aggregates,
        numerical_aggregation_columns,
        randstate,
        complex_predicates=complex_predicates,
    )
    having_clause = None
    if randstate.rand() < groupby_having_prob:
        idx = randstate.randint(0, len(aggregations))
        _, cols = aggregations[idx]
        literal = sum([vars(vars(column_stats)[col[0]])[col[1]].mean for col in cols])
        op = rand_choice(randstate, [Operator.LEQ, Operator.GEQ, Operator.NEQ])
        having_clause = (idx, literal, op)

    q = GenQuery(
        aggregations,
        group_bys,
        joins,
        predicates,
        start_t,
        list(join_tables),
        limit=limit,
        having_clause=having_clause,
        full_outer_join=full_outer_join,
    )
    return q


def generate_predicates(
    column_stats,
    complex_predicates,
    group_by_threshold,
    int_neq_predicate_threshold,
    join_tables,
    no_predicates,
    randstate,
    string_stats,
):
    (
        numerical_aggregation_columns,
        possible_columns,
        possible_string_columns,
        possible_group_by_columns,
        table_predicates,
        string_table_predicates,
    ) = analyze_columns(
        column_stats, group_by_threshold, join_tables, string_stats, complex_predicates
    )
    if complex_predicates:
        predicates = sample_complex_predicates(
            column_stats,
            string_stats,
            int_neq_predicate_threshold,
            no_predicates,
            possible_columns,
            possible_string_columns,
            table_predicates,
            string_table_predicates,
            randstate,
        )
    else:
        predicates = sample_predicates(
            column_stats,
            int_neq_predicate_threshold,
            no_predicates,
            possible_columns,
            table_predicates,
            randstate,
        )
    return numerical_aggregation_columns, possible_group_by_columns, predicates


class GenQuery:
    def __init__(
        self,
        aggregations,
        group_bys,
        joins,
        predicates,
        start_t,
        join_tables,
        alias_dict=None,
        inner_groupby=None,
        subquery_alias=None,
        limit=None,
        having_clause=None,
        full_outer_join=False,
    ):
        if alias_dict is None:
            alias_dict = dict()
        self.aggregations = aggregations
        self.group_bys = group_bys
        self.joins = joins
        self.predicates = predicates
        self.start_t = start_t
        self.join_tables = join_tables
        self.alias_dict = alias_dict
        self.exists_predicates = []
        self.inner_groupby = inner_groupby
        self.subquery_alias = subquery_alias
        self.limit = limit
        self.having_clause = having_clause
        self.full_outer_join = False
        if self.inner_groupby is not None:
            self.alias_dict = {t: subquery_alias for t in self.join_tables}

    def append_exists_predicate(self, q_rec, not_exist):
        self.exists_predicates.append((q_rec, not_exist))

    def check_every_table_has_predicate(self, tables, sql):
        if len(tables) == 0:
            return sql
        else:
            added_filter = []
            for table in tables:
                if table in self.alias_dict:
                    ta = self.alias_dict[table]
                else:
                    ta = table
                if sql.count(f'"{ta}"') == 2:
                    # this left outer join will not be necessary on this table because of no other filters
                    # randomly add a dummy filter
                    if type(tables[table]) == list:
                        col = tables[table][0]
                    else:
                        col = tables[table]
                    added_filter.append(f'"{ta}"."{col}" IS NOT NULL')
            if len(added_filter) != 0:
                added_sql = " AND ".join(added_filter)
                sql = sql[:-1] + " AND " + added_sql + ";"
            return sql

    def generate_sql_query(self, semicolon=True):
        # group_bys
        group_by_str = ""
        order_by_str = ""

        group_by_cols = []
        if len(self.group_bys) > 0:
            group_by_cols = [
                f'"{table}"."{column}"' for table, column, _ in self.group_bys
            ]
            group_by_col_str = ", ".join(group_by_cols)
            group_by_str = f" GROUP BY {group_by_col_str}"
            order_by_str = f" ORDER BY {group_by_col_str}"

        # aggregations
        aggregation_str_list = []
        for i, (aggregator, columns) in enumerate(self.aggregations):
            if aggregator == Aggregator.COUNT:
                aggregation_str_list.append(f"COUNT(*)")
            else:
                agg_cols = " + ".join([f'"{table}"."{col}"' for table, col in columns])
                aggregation_str_list.append(f"{str(aggregator)}({agg_cols})")
        aggregation_str = ", ".join(
            group_by_cols
            + [f"{agg} as agg_{i}" for i, agg in enumerate(aggregation_str_list)]
        )
        if aggregation_str == "":
            aggregation_str = "*"

        # having clause
        having_str = ""
        if self.having_clause is not None:
            idx, literal, op = self.having_clause
            having_str = f" HAVING {aggregation_str_list[idx]} {str(op)} {literal}"

        # predicates
        predicate_str = str(self.predicates)

        # other parts can simply be replaced with aliases
        for t, alias_t in self.alias_dict.items():
            predicate_str = predicate_str.replace(f'"{t}"', alias_t)
            aggregation_str = aggregation_str.replace(f'"{t}"', alias_t)
            group_by_str = group_by_str.replace(f'"{t}"', alias_t)
            order_by_str = order_by_str.replace(f'"{t}"', alias_t)
            having_str = having_str.replace(f'"{t}"', alias_t)

        if len(self.exists_predicates) > 0:
            exists_preds = []
            for q_rec, not_exist in self.exists_predicates:
                if not_exist:
                    exists_preds.append(
                        f"NOT EXISTS ({q_rec.generate_sql_query(semicolon=False)})"
                    )
                else:
                    exists_preds.append(
                        f"EXISTS ({q_rec.generate_sql_query(semicolon=False)})"
                    )
            exists_preds = " AND ".join(exists_preds)
            if predicate_str == "":
                predicate_str += f" WHERE {exists_preds} "
            else:
                predicate_str += f" AND {exists_preds}"

        check_tables = dict()
        # join
        if self.inner_groupby is not None:
            join_str = f"({self.inner_groupby.generate_sql_query(semicolon=False)}) {self.subquery_alias}"

        else:
            already_repl = set()

            def repl_alias(t, no_alias_intro=False):
                if t in self.alias_dict:
                    alias_t = self.alias_dict[t]
                    if t in already_repl or no_alias_intro:
                        return alias_t

                    else:
                        return f'"{t}" {alias_t}'

                return f'"{t}"'

            join_str = repl_alias(self.start_t)
            for table_l, column_l, table_r, column_r, left_outer in self.joins:
                if self.full_outer_join:
                    join_kw = "FULL OUTER JOIN"
                else:
                    join_kw = "JOIN" if left_outer else "LEFT OUTER JOIN"
                if join_kw == "LEFT OUTER JOIN":
                    check_tables[table_r] = column_r
                join_str += f" {join_kw} {repl_alias(table_r)}"
                join_cond = " AND ".join(
                    [
                        f'{repl_alias(table_l, no_alias_intro=True)}."{col_l}" = '
                        f'{repl_alias(table_r, no_alias_intro=True)}."{col_r}"'
                        for col_l, col_r in zip(column_l, column_r)
                    ]
                )
                join_str += f" ON {join_cond}"

        limit_str = ""
        if self.limit is not None:
            limit_str = f" LIMIT {self.limit}"

        sql_query = f"SELECT {aggregation_str} FROM {join_str} {predicate_str}{group_by_str}{having_str}{order_by_str}{limit_str}".strip()

        if semicolon:
            sql_query += ";"

        sql_query = self.check_every_table_has_predicate(check_tables, sql_query)
        return sql_query


def generate_workload(
    dataset,
    target_path,
    num_queries=100,
    max_no_joins=3,
    min_no_joins=0,
    max_no_predicates=3,
    min_no_predicates=0,
    max_no_aggregates=3,
    max_no_group_by=3,
    max_cols_per_agg=2,
    group_by_threshold=10000,
    int_neq_predicate_threshold=100,
    seed=0,
    complex_predicates=False,
    force=False,
    max_no_joins_static=False,
    max_no_aggregates_static=False,
    max_no_predicates_static=False,
    max_no_group_by_static=False,
    left_outer_join_ratio=0.0,
    groupby_limit_prob=0.0,
    groupby_having_prob=0.0,
    exists_predicate_prob=0.0,
    max_no_exists=0,
    outer_groupby_prob=0.0,
    no_joins_dist=[],
    full_outer_join=False,
):
    randstate = np.random.RandomState(seed)

    if os.path.exists(target_path) and not force:
        print("Workload already generated")
        return

    # read the schema file
    column_stats = load_column_statistics(dataset)
    string_stats = load_string_statistics(dataset)
    schema = load_schema_json(dataset)

    # build index of join relationships
    relationships_table = collections.defaultdict(list)
    for table_l, column_l, table_r, column_r in schema.relationships:
        if not isinstance(column_l, list):
            column_l = [column_l]
        if not isinstance(column_r, list):
            column_r = [column_r]

        relationships_table[table_l].append([column_l, table_r, column_r])
        relationships_table[table_r].append([column_r, table_l, column_l])

    queries = []
    no_joins_list = []
    no_joins_hist = [0 for _ in range(max_no_joins + 1)]
    for i in range(num_queries):
        # sample query as long as it does not meet requirements
        tries = 0
        desired_query = False
        while not desired_query:
            q = sample_acyclic_aggregation_query(
                column_stats,
                string_stats,
                group_by_threshold,
                int_neq_predicate_threshold,
                max_cols_per_agg,
                max_no_aggregates,
                max_no_group_by,
                max_no_joins,
                min_no_joins,
                max_no_predicates,
                min_no_predicates,
                relationships_table,
                schema,
                randstate,
                complex_predicates,
                max_no_joins_static,
                max_no_aggregates_static,
                max_no_predicates_static,
                max_no_group_by_static,
                left_outer_join_ratio,
                groupby_limit_prob,
                groupby_having_prob,
                full_outer_join=full_outer_join,
            )

            # retry maybe
            desired_query |= check_matches_criteria(
                q,
                complex_predicates,
                max_no_aggregates,
                max_no_aggregates_static,
                max_no_group_by,
                max_no_group_by_static,
                max_no_joins,
                max_no_joins_static,
                max_no_predicates,
                max_no_predicates_static,
            )

            # samples subqueries (self joins) for exists / not exists predicates and adds to query
            sample_exists_subqueries(
                column_stats,
                complex_predicates,
                exists_predicate_prob,
                group_by_threshold,
                int_neq_predicate_threshold,
                max_no_exists,
                q,
                randstate,
                relationships_table,
                string_stats,
                full_outer_join=full_outer_join,
            )

            # potentially sample outer query with another group by
            outer_groupby = randstate.rand() < outer_groupby_prob
            if outer_groupby:
                q = sample_outer_groupby(
                    complex_predicates, q, randstate, full_outer_join=full_outer_join
                )

            if desired_query:
                sql_query = q.generate_sql_query()
                queries.append(sql_query)
                no_joins = sql_query.count("JOIN")
                no_joins_list.append(no_joins)
                no_joins_hist[no_joins] += 1
                break
            else:
                tries += 1
                if tries > 10000:
                    raise ValueError(
                        "Did not find a valid query after 10000 trials. "
                        "Please check if your conditions can be fulfilled"
                    )

    # Sample queries to fulfill distribution
    keep = [True for _ in range(len(queries))]
    count_per_no_joins = no_joins_hist
    if no_joins_dist and len(no_joins_dist) != 0:
        # Normalize distribution
        s = sum(no_joins_dist)
        no_joins_dist = [i / s for i in no_joins_dist]

        # How many queries should we keep for each number of joins?
        max_feasible_total_queries = min(np.divide(no_joins_hist, no_joins_dist))
        count_per_no_joins = [
            int(i * max_feasible_total_queries) for i in no_joins_dist
        ]

        # Set appropriate flags in `keep`
        seen_per_no_joins = [0 for _ in no_joins_hist]
        for i in range(len(queries)):
            joins = no_joins_list[i]
            if seen_per_no_joins[joins] < count_per_no_joins[joins]:
                seen_per_no_joins[joins] += 1
            else:
                keep[i] = False

    target_dir = os.path.dirname(target_path)
    os.makedirs(target_dir, exist_ok=True)
    with open(target_path, "w") as text_file:
        text_file.write("\n".join([q for k, q in zip(keep, queries) if k]))
    with open(target_path + "_histogram", "w") as text_file:
        text_file.write(",".join([str(i) for i in count_per_no_joins]) + "\n")


def sample_outer_groupby(complex_predicates, q, randstate, full_outer_join=False):
    subquery_alias = "subgb"
    outer_aggs = []
    for i, (_, cols) in enumerate(q.aggregations):
        l = list(Aggregator)
        if complex_predicates:
            l += list(ExtendedAggregator)
        agg_type = rand_choice(randstate, l)
        outer_aggs.append((agg_type, [[subquery_alias, f"agg_{i}"]]))
    outer_groupby = []
    if len(q.group_bys) > 0:
        outer_groupby = rand_choice(
            randstate,
            q.group_bys,
            no_elements=randstate.randint(0, len(q.group_bys)),
            replace=False,
        )
        outer_groupby = [(subquery_alias, c, x) for _, c, x in outer_groupby]
    q = GenQuery(
        outer_aggs,
        outer_groupby,
        [],
        PredicateOperator(LogicalOperator.AND, []),
        None,
        q.join_tables,
        inner_groupby=q,
        subquery_alias=subquery_alias,
        full_outer_join=full_outer_join,
    )
    return q


def sample_exists_subqueries(
    column_stats,
    complex_predicates,
    exists_ratio,
    group_by_threshold,
    int_neq_predicate_threshold,
    max_no_exists,
    q,
    randstate,
    relationships_table,
    string_stats,
    full_outer_join=False,
):
    exists_subquery = randstate.rand() < exists_ratio
    eligible_exist = list(set(q.join_tables).intersection(relationships_table.keys()))
    if exists_subquery and len(eligible_exist) > 0:

        no_exists = randstate.randint(1, max_no_exists + 1)

        alias_dict = dict()
        exist_tables = []
        chosen_aliases = set()

        for _ in range(no_exists):
            alias_table = randstate.choice(eligible_exist)

            if alias_table not in alias_dict:
                alias_dict[alias_table] = f"{alias_table.lower()}_1"
            chosen_aliases.add(alias_dict[alias_table])

            for i in range(2, int(1e10)):
                subquery_alias = f"{alias_table.lower()}_{i}"
                if subquery_alias not in chosen_aliases:
                    rec_alias_dict = {alias_table: subquery_alias}
                    exist_tables.append((alias_table, rec_alias_dict))
                    chosen_aliases.add(subquery_alias)
                    break

        q.alias_dict = alias_dict

        # for each table generate exists subquery
        for t, rec_alias_dict in exist_tables:
            no_rec_pred = randstate.randint(1, 3)
            _, _, predicates = generate_predicates(
                column_stats,
                complex_predicates,
                group_by_threshold,
                int_neq_predicate_threshold,
                [t],
                no_rec_pred,
                randstate,
                string_stats,
            )
            possible_cols = set()
            for ct, _, _ in relationships_table[t]:
                possible_cols.update(ct)
            if len(possible_cols) == 0:
                continue
            key_exist_col = randstate.choice(list(possible_cols))

            op = randstate.choice([Operator.EQ, Operator.NEQ])
            self_pred = ColumnPredicate(
                t, key_exist_col, op, f'{alias_dict[t]}."{key_exist_col}"'
            )
            if type(predicates) == ColumnPredicate or len(predicates.children) > 0:
                p = PredicateOperator(LogicalOperator.AND, [predicates, self_pred])
            else:
                p = self_pred

            q_rec = GenQuery(
                [],
                [],
                [],
                p,
                t,
                [t],
                alias_dict=rec_alias_dict,
                full_outer_join=full_outer_join,
            )
            q.append_exists_predicate(q_rec, randstate.choice([True, False]))


def check_matches_criteria(
    q,
    complex_predicates,
    max_no_aggregates,
    max_no_aggregates_static,
    max_no_group_by,
    max_no_group_by_static,
    max_no_joins,
    max_no_joins_static,
    max_no_predicates,
    max_no_predicates_static,
):
    desired_query = True
    if (
        (max_no_joins_static and len(q.joins) < max_no_joins)
        or (max_no_aggregates_static and len(q.aggregations) < max_no_aggregates)
        or (max_no_group_by_static and len(q.group_bys) < max_no_group_by)
    ):
        desired_query = False
    if max_no_predicates_static:
        if complex_predicates:
            raise NotImplementedError("Check not implemented for complex predicates")
        else:
            if len(q.predicates.children) != max_no_predicates:
                desired_query = False
    return desired_query


def sample_group_bys(no_group_bys, possible_group_by_columns, randstate):
    group_bys = []
    if no_group_bys > 0:
        no_group_bys = min(no_group_bys, len(possible_group_by_columns))
        group_bys = rand_choice(
            randstate,
            possible_group_by_columns,
            no_elements=no_group_bys,
            replace=False,
        )
        # group_bys = randstate.sample(possible_group_by_columns, no_group_bys)
    return group_bys


def sample_aggregations(
    max_cols_per_agg,
    no_aggregates,
    numerical_aggregation_columns,
    randstate,
    complex_predicates=False,
):
    aggregations = []
    if no_aggregates > 0:
        for i in range(no_aggregates):
            no_agg_cols = min(
                randstate.randint(1, max_cols_per_agg + 1),
                len(numerical_aggregation_columns),
            )
            l = list(Aggregator)
            if complex_predicates:
                l += list(ExtendedAggregator)
            agg = rand_choice(randstate, l)
            cols = rand_choice(
                randstate,
                numerical_aggregation_columns,
                no_elements=no_agg_cols,
                replace=False,
            )
            # cols = randstate.sample(numerical_aggregation_columns, no_agg_cols)
            if agg == Aggregator.COUNT:
                cols = []
            if no_agg_cols == 0 and agg != Aggregator.COUNT:
                continue

            aggregations.append((agg, cols))
        if len(aggregations) == 0:
            aggregations.append((Aggregator.COUNT, []))
    return aggregations


class ColumnPredicate:
    def __init__(self, table, col_name, operator, literal):
        self.table = table
        self.col_name = col_name
        self.operator = operator
        self.literal = literal

    def __str__(self):
        return self.to_sql(top_operator=True)

    def to_sql(self, top_operator=False):
        if self.operator == Operator.IS_NOT_NULL:
            predicates_str = f'"{self.table}"."{self.col_name}" IS NOT NULL'
        elif self.operator == Operator.IS_NULL:
            predicates_str = f'"{self.table}"."{self.col_name}" IS NULL'
        else:
            predicates_str = (
                f'"{self.table}"."{self.col_name}" {str(self.operator)} {self.literal}'
            )

        if top_operator:
            predicates_str = f" WHERE {predicates_str}"

        return predicates_str


class LogicalOperator(Enum):
    AND = "AND"
    OR = "OR"

    def __str__(self):
        return self.value


class PredicateOperator:
    def __init__(self, logical_op, children=None):
        self.logical_op = logical_op
        if children is None:
            children = []
        self.children = children

    def __str__(self):
        return self.to_sql(top_operator=True)

    def to_sql(self, top_operator=False):
        sql = ""
        if len(self.children) > 0:
            # if len(self.children) == 1:
            #     return self.children[0].to_sql(top_operator=top_operator)

            predicates_str_list = [c.to_sql() for c in self.children]
            sql = f" {str(self.logical_op)} ".join(predicates_str_list)

            if top_operator:
                sql = f" WHERE {sql}"
            elif len(self.children) > 1:
                sql = f"({sql})"

        return sql


class PredicateChain(Enum):
    SIMPLE = "SIMPLE"
    OR_OR = "OR_OR"
    OR = "OR"
    OR_AND = "OR_AND"

    def __str__(self):
        return self.value


def sample_complex_predicates(
    column_stats,
    string_stats,
    int_neq_predicate_threshold,
    no_predicates,
    possible_columns,
    possible_string_columns,
    table_predicates,
    string_table_predicates,
    randstate,
    p_or=0.05,
    p_or_or=0.05,
    p_or_and=0.05,
    p_second_column=0.5,
):
    # weight the prob of being sampled by number of columns in table
    # make sure we do not just have conditions on one table with many columns
    weights = [1 / table_predicates[t] for t, col_name in possible_columns]
    weights += [
        1 / string_table_predicates[t] for t, col_name in possible_string_columns
    ]
    weights = np.array(weights)
    weights /= np.sum(weights)

    possible_columns += possible_string_columns
    no_predicates = min(no_predicates, len(possible_columns))
    predicate_col_idx = randstate.choice(
        range(len(possible_columns)), no_predicates, p=weights, replace=False
    )
    predicate_columns = [possible_columns[i] for i in predicate_col_idx]
    predicates = []
    for [t, col_name] in predicate_columns:

        # sample which predicate chain
        predicate_options = [
            PredicateChain.SIMPLE,
            PredicateChain.OR,
            PredicateChain.OR_OR,
            PredicateChain.OR_AND,
        ]
        pred_weights = [1 - p_or - p_or_or - p_or_and, p_or, p_or_or, p_or_and]
        pred_chain_idx = randstate.choice(
            range(len(predicate_options)), 1, p=pred_weights
        )[0]
        pred_chain = predicate_options[pred_chain_idx]

        # sample first predicate
        p = sample_predicate(
            string_stats,
            column_stats,
            t,
            col_name,
            int_neq_predicate_threshold,
            randstate,
            complex_predicate=True,
        )
        if p is None:
            continue

        if pred_chain == PredicateChain.SIMPLE:
            predicates.append(p)
        else:
            # sample if we use another column condition
            second_column = randstate.uniform() < p_second_column
            if second_column:
                potential_2nd_col = [
                    c2
                    for t2, c2 in possible_columns
                    if t2 == t and c2 != col_name and [t2, c2] not in predicate_columns
                ]
                if len(potential_2nd_col) == 0:
                    continue
                second_col = rand_choice(randstate, potential_2nd_col)
                p2 = sample_predicate(
                    string_stats,
                    column_stats,
                    t,
                    second_col,
                    int_neq_predicate_threshold,
                    randstate,
                    complex_predicate=True,
                )
            else:
                p2 = sample_predicate(
                    string_stats,
                    column_stats,
                    t,
                    col_name,
                    int_neq_predicate_threshold,
                    randstate,
                    complex_predicate=True,
                )
            if p2 is None:
                continue

            complex_pred = None
            if pred_chain == PredicateChain.OR:
                complex_pred = PredicateOperator(LogicalOperator.OR, [p, p2])
            else:
                p3 = sample_predicate(
                    string_stats,
                    column_stats,
                    t,
                    col_name,
                    int_neq_predicate_threshold,
                    randstate,
                    complex_predicate=True,
                )
                if p3 is None:
                    complex_pred = PredicateOperator(LogicalOperator.OR, [p, p2])
                else:
                    if pred_chain == PredicateChain.OR_OR:
                        complex_pred = PredicateOperator(
                            LogicalOperator.OR, [p, p2, p3]
                        )
                    elif pred_chain == PredicateChain.OR_AND:
                        complex_pred = PredicateOperator(
                            LogicalOperator.OR,
                            [p, PredicateOperator(LogicalOperator.AND, [p2, p3])],
                        )
            predicates.append(complex_pred)

    if len(predicates) == 1:
        return predicates[0]

    return PredicateOperator(LogicalOperator.AND, predicates)


def sample_predicate(
    string_stats,
    column_stats,
    t,
    col_name,
    int_neq_predicate_threshold,
    randstate,
    complex_predicate=False,
    p_like=0.5,
    p_is_not_null=0.1,
    p_in=0.5,
    p_between=0.3,
    p_not_like=0.5,
    p_mid_string_whitespace=0.5,
):
    col_stats = vars(vars(column_stats)[t]).get(col_name)
    str_stats = None
    if string_stats is not None:
        str_stats = vars(vars(string_stats)[t]).get(col_name)

    if complex_predicate:

        # LIKE / NOT LIKE
        if (
            col_stats is None
            or col_stats.datatype == str(Datatype.MISC)
            or (str_stats is not None and randstate.uniform() < p_like)
        ):
            freq_words = [w for w in str_stats.freq_str_words if len(w) > 1]
            if len(freq_words) == 0:
                return None
            literal = rand_choice(randstate, freq_words)

            # additional whitespace in the middle
            if randstate.uniform() < p_mid_string_whitespace:
                split_pos = randstate.randint(1, len(literal))
                literal = literal[:split_pos] + "%" + literal[split_pos:]

            if type(literal) == str and "'" in literal:
                literal = f"''%{literal}%''"
            else:
                literal = f"'%{literal}%'"

            if randstate.uniform() < p_not_like:
                op = Operator.NOT_LIKE
            else:
                op = Operator.LIKE

            return ColumnPredicate(t, col_name, op, literal)

        # IS NOT NULL / IS NULL
        if col_stats.nan_ratio > 0 and randstate.uniform() < p_is_not_null:
            if randstate.uniform() < 0.8:
                return ColumnPredicate(t, col_name, Operator.IS_NOT_NULL, None)
            return ColumnPredicate(t, col_name, Operator.IS_NULL, None)

        # IN
        if (
            col_stats.datatype == str(Datatype.CATEGORICAL)
            and randstate.uniform() < p_in
        ):
            # rand_choice(randstate, l, no_elements=None, replace=False)
            literals = col_stats.unique_vals
            first_cap = min(len(literals), 10)
            literals = literals[:first_cap]

            if len(literals) <= 1:
                return None

            no_in_literals = randstate.randint(1, len(literals))
            literals = rand_choice(
                randstate, literals, no_elements=no_in_literals, replace=False
            )
            literals = ", ".join(
                [
                    f"''{l}''" if (type(l) == str and "'" in l) else f"'{l}'"
                    for l in literals
                ]
            )
            literals = f"({literals})"

            return ColumnPredicate(t, col_name, Operator.IN, literals)

        if (
            col_stats.datatype in {str(Datatype.INT), str(Datatype.FLOAT)}
            and randstate.uniform() < p_between
        ):
            l1 = sample_literal_from_percentiles(
                col_stats.percentiles,
                randstate,
                round=col_stats.datatype == str(Datatype.INT),
            )
            l2 = sample_literal_from_percentiles(
                col_stats.percentiles,
                randstate,
                round=col_stats.datatype == str(Datatype.INT),
            )
            if l1 == l2:
                l2 += 1
            literal = f"{min(l1, l2)} AND {max(l1, l2)}"
            return ColumnPredicate(t, col_name, Operator.BETWEEN, literal)

    # simple predicates
    if col_stats.datatype == str(Datatype.INT):
        reasonable_ops = [Operator.LEQ, Operator.GEQ]
        if col_stats.num_unique < int_neq_predicate_threshold:
            reasonable_ops.append(Operator.EQ)
            reasonable_ops.append(Operator.NEQ)

        literal = sample_literal_from_percentiles(
            col_stats.percentiles, randstate, round=True
        )

    elif col_stats.datatype == str(Datatype.FLOAT):
        reasonable_ops = [Operator.LEQ, Operator.GEQ]
        literal = sample_literal_from_percentiles(
            col_stats.percentiles, randstate, round=False
        )
        # nan comparisons only produce errors
        # happens when column is all nan
        if np.isnan(literal):
            return None
    elif col_stats.datatype == str(Datatype.CATEGORICAL):
        reasonable_ops = [Operator.EQ, Operator.NEQ]
        possible_literals = [
            v
            for v in col_stats.unique_vals
            if v is not None and not (isinstance(v, float) and np.isnan(v))
        ]
        if len(possible_literals) == 0:
            return None
        literal = rand_choice(randstate, possible_literals)
        if type(literal) == str and "'" in literal:
            literal = f"''%{literal}%''"
        else:
            literal = f"'%{literal}%'"
    else:
        raise NotImplementedError
    operator = rand_choice(randstate, reasonable_ops)
    return ColumnPredicate(t, col_name, operator, literal)


def sample_predicates(
    column_stats,
    int_neq_predicate_threshold,
    no_predicates,
    possible_columns,
    table_predicates,
    randstate,
):
    # sample random predicates
    # weight the prob of being sampled by number of columns in table
    # make sure we do not just have conditions on one table with many columns
    weights = np.array([1 / table_predicates[t] for t, col_name in possible_columns])
    weights /= np.sum(weights)
    # we cannot sample more predicates than available columns
    no_predicates = min(no_predicates, len(possible_columns))
    predicate_col_idx = randstate.choice(
        range(len(possible_columns)), no_predicates, p=weights, replace=False
    )
    predicate_columns = [possible_columns[i] for i in predicate_col_idx]
    predicates = []
    for [t, col_name] in predicate_columns:
        p = sample_predicate(
            None,
            column_stats,
            t,
            col_name,
            int_neq_predicate_threshold,
            randstate,
            complex_predicate=False,
        )
        if p is not None:
            predicates.append(p)

    return PredicateOperator(LogicalOperator.AND, predicates)


def analyze_columns(
    column_stats, group_by_treshold, join_tables, string_stats, complex_predicates
):
    # find possible columns for predicates
    possible_columns = []
    possible_string_columns = []
    possible_group_by_columns = []
    numerical_aggregation_columns = []
    # also track how many columns we have per table to reweight them
    table_predicates = collections.defaultdict(int)
    string_table_predicates = collections.defaultdict(int)
    for t in join_tables:
        for col_name, col_stats in vars(vars(column_stats)[t]).items():
            if col_stats.datatype in {
                str(d) for d in [Datatype.INT, Datatype.FLOAT, Datatype.CATEGORICAL]
            }:
                possible_columns.append([t, col_name])
                table_predicates[t] += 1

            if complex_predicates and col_name in vars(vars(string_stats)[t]):
                possible_string_columns.append([t, col_name])
                string_table_predicates[t] += 1

            # group by columns
            if (
                col_stats.datatype
                in {str(d) for d in [Datatype.INT, Datatype.CATEGORICAL]}
                and col_stats.num_unique < group_by_treshold
            ):
                possible_group_by_columns.append([t, col_name, col_stats.num_unique])

            # numerical aggregation columns
            if col_stats.datatype in {str(d) for d in [Datatype.INT, Datatype.FLOAT]}:
                numerical_aggregation_columns.append([t, col_name])
    return (
        numerical_aggregation_columns,
        possible_columns,
        possible_string_columns,
        possible_group_by_columns,
        table_predicates,
        string_table_predicates,
    )


def sample_literal_from_percentiles(percentiles, randstate, round=False):
    start_idx = randstate.randint(0, len(percentiles) - 1)
    if np.all(np.isnan(percentiles)):
        return np.nan
    literal = randstate.uniform(percentiles[start_idx], percentiles[start_idx + 1])
    if round:
        literal = int(literal)
    return literal


def rand_choice(randstate, l, no_elements=None, replace=False):
    if no_elements is None:
        idx = randstate.randint(0, len(l))
        return l[idx]
    else:
        idxs = randstate.choice(range(len(l)), no_elements, replace=replace)
        return [l[i] for i in idxs]


def sample_acyclic_join(
    no_joins, relationships_table, schema, randstate, left_outer_join_ratio
):
    # randomly sample join
    joins = list()
    start_t = rand_choice(randstate, schema.tables)
    join_tables = {start_t}

    for i in range(no_joins):
        possible_joins = find_possible_joins(join_tables, relationships_table)

        # randomly select one join
        if len(possible_joins) > 0:
            t, column_l, table_r, column_r = rand_choice(randstate, possible_joins)
            join_tables.add(table_r)

            left_outer_join = False
            if left_outer_join_ratio > 0 and randstate.rand() < left_outer_join_ratio:
                left_outer_join = True

            joins.append((t, column_l, table_r, column_r, left_outer_join))
        else:
            break
    return start_t, joins, join_tables


def find_possible_joins(join_tables, relationships_table):
    possible_joins = list()
    for t in join_tables:
        for column_l, table_r, column_r in relationships_table[t]:
            if table_r in join_tables:
                continue
            possible_joins.append((t, column_l, table_r, column_r))
    return possible_joins
