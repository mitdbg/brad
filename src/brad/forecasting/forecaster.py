from brad.forecasting.query_parser import QueryParser
import argparse
import os
from sqlglot import exp
import sys

MAX_JOINS = 10


class WorkloadForecaster:
    def __init__(self):
        self._parser = QueryParser()
        self._num_joins_histogram = [0 for _ in range(MAX_JOINS + 1)]
        self._total_queries = 0

    def get_template_frequency(self, num_joins):
        return self._num_joins_histogram[num_joins] / self._total_queries

    def forecast(self):
        print("I am a placeholder")
        return

    # def process(self, sql_query):
    #     clause_dict = self._parser.get_clauses(sql_query.rstrip(";"))
    #
    #     (
    #         join_predicates,
    #         filtered_attributes,
    #     ) = self._parser.get_predicates_and_filtered_attributes(clause_dict)
    #
    #     self._num_joins_histogram[len(join_predicates)] += 1
    #     self._total_queries += 1
    #
    #     return (
    #         join_predicates,
    #         len(join_predicates),
    #         filtered_attributes,
    #         len(filtered_attributes),
    #     )

    def process(self, query_rep):
        parsed = query_rep.ast()

        # Count joins
        tables_cnt = 0
        for _ in parsed.find_all(exp.Table):
            tables_cnt += 1

        self._num_joins_histogram[tables_cnt - 1] += 1
        self._total_queries += 1

    def print_histogram(self):
        for i in range(len(self._num_joins_histogram)):
            count_i = self._num_joins_histogram[i]
            freq_i = self.get_template_frequency(i) * 100
            print(
                f"{i:02d} Join(s): {count_i:06d} ({freq_i:05.2f}%) " + "|" * int(freq_i)
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--in_dir", type=str, default=".")
    parser.add_argument("--filename", type=str, default=None)
    args = parser.parse_args()

    forecaster = WorkloadForecaster()

    if args.filename:
        with open(args.filename, "r", encoding="utf8") as f:
            for q in f:
                try:
                    forecaster.process(q)
                except:  # pylint: disable=bare-except
                    print(q)
                    sys.exit(1)
    else:
        for name in os.listdir(args.in_dir):
            filename = os.path.join(args.in_dir, name)
            if os.path.isfile(filename) and filename.endswith(".sql"):
                with open(filename, "r", encoding="utf8") as f:
                    for q in f:
                        forecaster.process(q)

    forecaster.print_histogram()
