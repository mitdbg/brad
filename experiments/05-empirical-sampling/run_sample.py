import argparse
import pandas as pd
import numpy as np

# This script is used with a blueprint scoring log. The aim is to empirically
# verify our theoretical calculations on the number of samples we need to take
# to have a high liklihood (> 99.9%) of selecting a "good" blueprint.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-file", type=str, required=True)
    parser.add_argument("--num-samples", type=int, required=True)
    parser.add_argument("--num-trials", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--score-threshold", type=float, default=0.9)
    args = parser.parse_args()

    df = pd.read_csv(args.log_file)
    np.random.seed(args.seed)

    matching = df[df["single_value"] <= args.score_threshold]
    print("Number of 'good' choices: {}".format(len(matching)))

    num_found = 0
    for _ in range(args.num_trials):
        df_sample = df.sample(n=args.num_samples)
        good = df_sample[df_sample["single_value"] <= args.score_threshold]
        if len(good) > 0:
            num_found += 1

    print("Successes: {}, Total: {}".format(num_found, args.num_trials))
    print("Success prob: {:.2f}".format(num_found / args.num_trials))


if __name__ == "__main__":
    main()
