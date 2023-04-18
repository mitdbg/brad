#! /usr/bin/python

import argparse
import boto3
import botocore
import json

from typing import List, Dict


# The AWS API does not return spec results for these instances (probably because
# they are not available in EC2).
# https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.DBInstanceClass.html
X2G_INSTANCES = [
    {
        "instance_type": "db.x2g.large",
        "vcpus": 2,
        "memory_mib": 32 * 1024,
    },
    {
        "instance_type": "db.x2g.xlarge",
        "vcpus": 4,
        "memory_mib": 64 * 1024,
    },
    {
        "instance_type": "db.x2g.2xlarge",
        "vcpus": 8,
        "memory_mib": 128 * 1024,
    },
    {
        "instance_type": "db.x2g.4xlarge",
        "vcpus": 16,
        "memory_mib": 256 * 1024,
    },
    {
        "instance_type": "db.x2g.8xlarge",
        "vcpus": 32,
        "memory_mib": 512 * 1024,
    },
    {
        "instance_type": "db.x2g.12xlarge",
        "vcpus": 48,
        "memory_mib": 768 * 1024,
    },
    {
        "instance_type": "db.x2g.16xlarge",
        "vcpus": 64,
        "memory_mib": 1024 * 1024,
    },
]


def get_instance_types() -> List[str]:
    # Set up the Aurora PostgreSQL client
    client = boto3.client("rds", region_name="us-east-1")

    # Retrieve the Aurora PostgreSQL instance types
    response = client.describe_orderable_db_instance_options(
        Engine="aurora-postgresql",
        EngineVersion="14.6",
    )

    # Extract the relevant information for each instance type
    instance_types = []
    for option in response["OrderableDBInstanceOptions"]:
        instance_types.append(option["DBInstanceClass"])

    return instance_types


def retrieve_specs(instance_types: List[str]) -> List[Dict[str, int | str]]:
    # Retrieve the hardware specifications for each instance type
    client = boto3.client("ec2", region_name="us-east-1")

    specs = []
    for instance_type in instance_types:
        # Remove the "db." prefix from the instance type name.
        adjusted = instance_type[3:]
        try:
            response = client.describe_instance_types(InstanceTypes=[adjusted])

            vcpu = response["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"]
            memory = response["InstanceTypes"][0]["MemoryInfo"]["SizeInMiB"]

            specs.append(
                {
                    "instance_type": instance_type,
                    "vcpus": vcpu,
                    "memory_mib": memory,
                }
            )
        except botocore.exceptions.ClientError:
            print("WARNING: No specs found for", instance_type)

    return specs


def spec_sort_key(spec):
    return (spec["vcpus"], spec["memory_mib"], spec["instance_type"])


def main():
    parser = argparse.ArgumentParser("Retrieve RDS instance specs from the AWS API.")
    parser.add_argument(
        "--output-file",
        type=str,
        default="../src/brad/planner/scoring/data/aurora_postgresql_instances.json",
    )
    args = parser.parse_args()

    instances = get_instance_types()
    specs = retrieve_specs(instances)
    specs.extend(X2G_INSTANCES)
    specs.sort(key=spec_sort_key)

    with open(args.output_file, "w", encoding="UTF-8") as out_file:
        json.dump(specs, out_file, indent=2)


if __name__ == "__main__":
    main()
