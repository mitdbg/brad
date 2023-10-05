import argparse
import random
from brad.query_rep import QueryRep


def compute_templates(queries):
    res = []
    counts = {}

    for q in queries:
        qr = QueryRep(q)
        tables = qr.tables()
        tables.sort()
        tables_tup = tuple(tables)

        res.append((q, tables_tup))

        if tables_tup not in counts:
            counts[tables_tup] = 1
        else:
            counts[tables_tup] += 1

    # Sort
    counts_list = [(*i,) for i in counts.items()]
    counts_list.sort(key=lambda i: i[1], reverse=True)

    return res, counts_list, counts


def main():
    # This script selects queries based on the most frequently occuring
    # templates (when considering table presence only).
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-file", type=str, required=True)
    parser.add_argument("--out-file", type=str, required=True)
    parser.add_argument("--num-queries", type=int, required=True)
    parser.add_argument("--num-templates", type=int, default=25)
    args = parser.parse_args()

    with open(args.query_file) as file:
        queries = [line.strip() for line in file]

    query_with_template, ordered_templates, _ = compute_templates(queries)

    # Print the templates first.
    for tpl, count in ordered_templates:
        print(tpl, count)

    # Relevant templates
    rel_templates = {tpl[0] for tpl in ordered_templates[: args.num_templates]}
    rel_queries = [q[0] for q in query_with_template if q[1] in rel_templates]

    random.seed(42)
    random.shuffle(rel_queries)

    with open(args.out_file, "w") as file:
        for q in rel_queries[: args.num_queries]:
            print(q, file=file)


if __name__ == "__main__":
    main()
