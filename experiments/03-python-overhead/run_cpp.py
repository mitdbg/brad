import argparse
import subprocess

from brad.config.file import ConfigFile
from brad.config.dbtype import DBType


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dbname", type=str, required=True)
    parser.add_argument("--config-file", type=str, required=True)
    parser.add_argument("--iters", type=int, default=50)
    args = parser.parse_args()

    config = ConfigFile(args.config_file)
    db = DBType.from_str(args.dbname)
    cstr = config.get_odbc_connection_string(db)

    subprocess.run(
        './build/native_overhead --dbname {} --cstr "{}" --iters {} > $COND_OUT/results.csv'.format(
            args.dbname, cstr, args.iters
        ),
        shell=True,
    )


if __name__ == "__main__":
    main()
