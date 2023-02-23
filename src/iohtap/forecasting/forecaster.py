from iohtap.forecasting.query_parser import QueryParser


class WorkloadForecaster:
    def forecast(self):
        print("I am a placeholder")
        return

    def process(self, sql_query):
        p = QueryParser()
        l = p.get_clauses(sql_query)
        return l
