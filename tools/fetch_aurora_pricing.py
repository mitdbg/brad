#! /usr/bin/python

import argparse
import boto3
import json


def append_pricing(instance_configs):
    # Retrieve the price for each instance type using the AWS Price List API
    pricing_client = boto3.client("pricing", region_name="us-east-1")

    for instance_config in instance_configs:
        response = pricing_client.get_products(
            ServiceCode="AmazonRDS",
            Filters=[
                {
                    "Type": "TERM_MATCH",
                    "Field": "databaseEngine",
                    "Value": "Aurora PostgreSQL",
                },
                {
                    "Type": "TERM_MATCH",
                    "Field": "instanceType",
                    "Value": instance_config["instance_type"],
                },
                {
                    "Type": "TERM_MATCH",
                    "Field": "location",
                    "Value": "US East (N. Virginia)",
                },
            ],
            MaxResults=2,
        )

        for pl in response["PriceList"]:
            inner = json.loads(pl)
            storage_type = inner["product"]["attributes"]["storage"]
            is_io_optimized = "IO Optimization" in storage_type
            rel = inner["terms"]["OnDemand"]
            rel = next(iter(rel.values()))
            rel = rel["priceDimensions"]
            rel = next(iter(rel.values()))
            hourly_pricing = rel["pricePerUnit"]["USD"]
            # results.append((instance_config["instance_type"], float(hourly_pricing)))
            if is_io_optimized:
                instance_config["io_opt_usd_per_hour"] = float(hourly_pricing)
            else:
                # N.B. `float` may not be the most ideal.
                instance_config["usd_per_hour"] = float(hourly_pricing)


def main():
    parser = argparse.ArgumentParser("Retrieve RDS instance pricing from the AWS API.")
    parser.add_argument(
        "--in-out-file",
        type=str,
        default="../src/brad/planner/scoring/data/aurora_postgresql_instances_full.json",
    )
    args = parser.parse_args()

    with open(args.in_out_file, "r", encoding="UTF-8") as file:
        instance_configs = json.load(file)

    append_pricing(instance_configs)

    with open(args.in_out_file, "w", encoding="UTF-8") as file:
        json.dump(instance_configs, file, indent=2)
        file.write("\n")


if __name__ == "__main__":
    main()
