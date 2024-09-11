# import pandas
# df = pandas.read_table('sf1/customer.tbl', sep="|")
# df.to_csv("sf1/customer.csv")

import json
import sys


def converttbldatatocsvformat(filename, header):
    csv = open("".join([filename, ".csv"]), "w+")
    csv.write(header + "\n")
    tbl = open("".join([filename, ".tbl"]), "r")
    lines = tbl.readlines()
    for line in lines:
        line = line[:-2] + line[-1:]  # remove trailing delimiter
        line = line.replace(",", "N")
        line = line.replace("|", ",")
        csv.write(line)
    tbl.close()
    csv.close()


# TODO: Need to change this path later
json_file = "/home/axing/brad/sandbox/qe/tpch/headers.json"
if __name__ == "__main__":
    # Load the schema definitions from the YAML file
    with open(json_file, "r") as f:
        schema = json.load(f)

    path = sys.argv[1]
    for f in schema.keys():
        converttbldatatocsvformat(path + "/" + f, schema[f])
