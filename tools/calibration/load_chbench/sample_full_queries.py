import argparse
import random


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-file", type=str, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-query-blocks", type=int, default=22)
    parser.add_argument("--queries-per-block", type=int, default=200)
    args = parser.parse_args()

    prng = random.Random(args.seed)

    with open(args.query_file, "r", encoding="UTF-8") as file:
        queries = [line.strip() for line in file]

    selected = []
    for qidx in range(args.num_query_blocks):
        offset = prng.randint(0, args.queries_per_block - 1)
        selected.append(queries[qidx * args.queries_per_block + offset])

    with open("selected_queries.sql", "w", encoding="UTF-8") as file:
        for q in selected:
            print(q, file=file)


if __name__ == "__main__":
    main()
