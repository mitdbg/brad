import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-file", type=str)
    parser.add_argument("--out-file", type=str)
    parser.add_argument("--indices", type=str, help="Comma separated.")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    with open(args.source_file, "r", encoding="UTF-8") as file:
        queries = [line.strip() for line in file]

    indices = [int(qidx_str.strip()) for qidx_str in args.indices.split(",")]

    mode = "w" if args.overwrite else "a"
    with open(args.out_file, mode, encoding="UTF-8") as out_file:
        for idx in indices:
            print(queries[idx], file=out_file)


if __name__ == "__main__":
    main()
