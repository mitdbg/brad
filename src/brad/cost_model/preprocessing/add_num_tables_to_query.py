import json
from workloads.cross_db_benchmark.benchmark_tools.utils import load_json


def add_num_tables(src_workloads):
    for src in src_workloads:
        try:
            run = load_json(src, namespace=False)
        except:
            raise ValueError(f"Error reading {src}")

        all_num_tables = []
        for q_id, p in enumerate(run["parsed_plans"]):
            tables = get_tables(p)
            assert len(tables) != 0
            p["num_tables"] = len(tables)
            all_num_tables.append(len(tables))

        with open(src, "w") as outfile:
            json.dump(run, outfile)
        print(
            f"done adding num_tables for {src}, with max num_table {max(all_num_tables)} min {min(all_num_tables)}"
        )


def get_tables(plan):
    tables = set()
    if "table" in plan["plan_parameters"]:
        tables.add(plan["plan_parameters"]["table"])

    for c in plan["children"]:
        c_tables = get_tables(c)
        tables.update(c_tables)

    return tables
