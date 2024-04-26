import pandas as pd
import os
import sys


def csv_to_parquet(path, input_file, output_dir):
    if input_file[-4:] != ".csv":
        return
    df = pd.read_csv(os.path.join(path, input_file))
    df.to_parquet(os.path.join(output_dir, input_file[:-4] + ".parquet"))


if __name__ == "__main__":
    path = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for f in os.listdir(path):
        if not os.path.isfile(os.path.join(path, f)):
            continue
        csv_to_parquet(path, f, output_dir)
