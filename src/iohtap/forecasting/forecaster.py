from iohtap.forecasting.query_parser import QueryParser


class WorkloadForecaster:
    def __init__(self):
        self._parser = QueryParser()
        self._num_joins_histogram = [0 for _ in range(11)]
        self._total_queries = 0

    def get_template_frequency(self, num_joins):
        return self._num_joins_histogram[num_joins] / self._total_queries

    def forecast(self):
        print("I am a placeholder")
        return

    def process(self, sql_query):
        clause_dict = self._parser.get_clauses(sql_query.rstrip(";"))

        (
            join_predicates,
            filtered_attributes,
        ) = self._parser.get_predicates_and_filtered_attributes(clause_dict)

        self._num_joins_histogram[len(join_predicates)] += 1
        self._total_queries += 1

        return (
            join_predicates,
            len(join_predicates),
            filtered_attributes,
            len(filtered_attributes),
        )


if __name__ == "__main__":
    q1 = "SELECT MIN(chn.name) AS uncredited_voiced_character, MIN(t.title) AS russian_movie FROM char_name AS chn, cast_info AS ci, company_name AS cn, company_type AS ct, movie_companies AS mc, role_type AS rt, title AS  t WHERE ci.note LIKE '%(voice)%' AND ci.note LIKE '%(uncredited)%' AND cn.country_code = '[ru]' AND rt.role = 'actor' AND t.production_year > 2005 AND t.id = mc.movie_id AND t.id = ci.movie_id AND ci.movie_id = mc.movie_id AND chn.id = ci.person_role_id AND rt.id = ci.role_id AND cn.id = mc.company_id AND ct.id = mc.company_type_id;"
    q2 = "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;"
    f = WorkloadForecaster()
    print(f.process(q1))
    print(f.process(q2))

    for i in range(11):
        print(f.get_template_frequency(i))
