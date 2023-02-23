from query_parser import QueryParser
import re


class WorkloadForecaster:
    def __init__(self):
        self._parser = QueryParser()

    def forecast(self):
        print("I am a placeholder")
        return

    def process(self, sql_query):
        clause_dict = self._parser.get_clauses(sql_query)

        # Count number of joins in the query. Joins may either be in the FROM clause
        # or in the WHERE clause.
        num_joins = 0

        if "FROM" in clause_dict.keys():
            num_joins += clause_dict["FROM"].count("JOIN")
        if "WHERE" in clause_dict.keys():
            # Match the pattern period - equals sign - period, with the periods not inside single quotes.
            num_joins += sum([int(bool(re.match(r"[^']*\.[^']*=[^']*\.[^']*", i))) for i in clause_dict["WHERE"].split("AND")])
        
        return num_joins
    

if __name__ == "__main__":
    s = "WHERE cn.country_code ='[us]' AND k.keyword ='character-name-in-.title' AND n.name LIKE 'B%' AND n.id = ci.person_id AND ci.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND ci.movie_id = mc.movie_id AND ci.movie_id = mk.movie_id AND mc.movie_id = mk.movie_id;"
    f = WorkloadForecaster()
    print(f.process(s))


