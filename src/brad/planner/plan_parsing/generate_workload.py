from enum import Enum


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
