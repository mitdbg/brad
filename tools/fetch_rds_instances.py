import argparse
import boto3
import botocore

from typing import List, Dict


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


def retrieve_specs(instance_types: List[str]) -> Dict[str, Dict[str, int]]:
    # Retrieve the hardware specifications for each instance type
    client = boto3.client("ec2", region_name="us-east-1")

    specs = {}
    for instance_type in instance_types:
        # Remove the "db." prefix from the instance type name.
        adjusted = instance_type[3:]
        try:
            response = client.describe_instance_types(InstanceTypes=[adjusted])

            vcpu = response["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"]
            memory = response["InstanceTypes"][0]["MemoryInfo"]["SizeInMiB"]

            specs[instance_type] = {
                "vcpu": vcpu,
                "memory_mib": memory,
            }
        except botocore.exceptions.ClientError:
            print("No info for", instance_type)

    return specs


def build_neighbor_graph(specs):
    processed = set()
    edges = []

    def is_better(existing, new, base):
        return new["vcpu"] < existing["vcpu"] and new["vcpu"] / base["vcpu"] > 1.0

    def is_same(existing, new):
        return new["vcpu"] == existing["vcpu"]

    for inst_type, spec in specs.items():
        if inst_type in processed:
            continue

        candidates = []

        for inner_inst_type, inner_spec in specs.items():
            if inner_inst_type == inst_type:
                continue

            if inner_spec["vcpu"] < spec["vcpu"]:
                continue

            if len(candidates) == 0:
                candidates.append((inner_inst_type, inner_spec))
            elif all(map(lambda cand: is_better(cand[1], inner_spec, spec), candidates)):
                candidates.clear()
                candidates.append((inner_inst_type, inner_spec))
            elif all(map(lambda cand: is_same(cand[1], inner_spec), candidates)):
                candidates.append((inner_inst_type, inner_spec))

        processed.add(inst_type)
        for c in candidates:
            edges.append((inst_type, c[0], c[1]["vcpu"] / spec["vcpu"]))

    return edges


def main():
    instances = get_instance_types()
    specs = retrieve_specs(instances)

    for inst, spec in specs.items():
        print(inst)
        print(spec)

    print()
    graph_edges = build_neighbor_graph(specs)
    for e in graph_edges:
        print(e)


if __name__ == "__main__":
    main()
