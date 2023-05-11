import argparse
import random
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-trials", type=int, default=1_000_000)
    parser.add_argument("--integer-range", type=int, default=100_000)
    args = parser.parse_args()

    start_time = time.monotonic()

    for _ in range(args.num_trials):
        random.randint(0, args.integer_range)

    elapsed_time = time.monotonic() - start_time

    throughput = args.num_trials / elapsed_time
    print(f"Throughput: {throughput:.3f} integers per second")


if __name__ == "__main__":
    main()
