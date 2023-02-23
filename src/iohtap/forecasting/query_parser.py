import re

CLAUSE_KEYWORDS = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY"]


class QueryParser:
    def __init__(self):
        self._keywords = [i.lower() for i in CLAUSE_KEYWORDS]
        self._keywords.extend(CLAUSE_KEYWORDS)

    # Splits a string representing a SQL query into a dictionary of clauses.
    # The keys are keywords from CLAUSE_KEYWORDS and the values are the corresponding clauses.
    def get_clauses(self, sql_query):
        l = re.split(f"({'|'.join(self._keywords)})", sql_query)
        d = {}
        for i, item in enumerate(l):
            if item in self._keywords:
                d[item.upper()] = l[i + 1].strip()

        return d

    def get_predicates_and_filtered_attributes(self, d):
        # Find join predicates in the query. Joins may either be in the FROM clause
        # or in the WHERE clause.
        predicates = []

        # Also find the attributes that are targets of filters in the queries
        filtered_attributes = []

        if "FROM" in d.keys():
            tables = d["FROM"].split(",")
            predicates.extend(
                [t.split("ON")[1].strip() for t in tables if len(t.split("ON")) >= 2]
            )
        if "WHERE" in d.keys():
            # Match the pattern period - equals sign - period, with the periods not inside single quotes.

            for i in d["WHERE"].split("AND"):
                if bool(re.match(r"[^']*\.[^']*=[^']*\.[^']*", i)):
                    predicates.append(i.strip())
                else:
                    filtered_attributes.append(i.split()[0].strip())

        return predicates, filtered_attributes


if __name__ == "__main__":
    s = "SELECT * FROM A where A.x=2"
    p = QueryParser()
    print(p.get_clauses(s))
