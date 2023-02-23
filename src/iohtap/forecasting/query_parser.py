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


if __name__ == "__main__":
    s = "SELECT * FROM A where A.x=2"
    p = QueryParser()
    print(p.get_clauses(s))
