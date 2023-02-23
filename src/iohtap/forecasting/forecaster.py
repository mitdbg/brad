from iohtap.forecasting.query_parser import QueryParser


class WorkloadForecaster:
    def __init__(self):
        self._parser = QueryParser()

    def forecast(self):
        print("I am a placeholder")
        return

    def process(self, sql_query):
        clause_dict = self._parser.get_clauses(sql_query)

        num_joins = self._parser.get_num_joins(clause_dict)

        return num_joins


if __name__ == "__main__":
    s = "WHERE cn.country_code ='[us]' AND k.keyword ='character-name-in-.title' AND n.name LIKE 'B%' AND n.id = ci.person_id AND ci.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND ci.movie_id = mc.movie_id AND ci.movie_id = mk.movie_id AND mc.movie_id = mk.movie_id;"
    f = WorkloadForecaster()
    print(f.process(s))
