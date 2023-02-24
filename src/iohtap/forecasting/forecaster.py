from iohtap.forecasting.query_parser import QueryParser
import argparse
import os


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
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--in_dir", type=str, default=".")
    args = parser.parse_args()

    forecaster = WorkloadForecaster()

    for name in os.listdir(args.in_dir):
        filename = os.path.join(args.in_dir, name)
        if os.path.isfile(filename) and filename.endswith(".sql"):
            with open(filename, "r", encoding="utf8") as f:
                for q in f:
                    print(forecaster.process(q))

    for i in range(11):
        print(forecaster.get_template_frequency(i))
