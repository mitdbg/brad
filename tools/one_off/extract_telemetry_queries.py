import json
from typing import Dict, List


def process_file(filename: str) -> Dict[str, List[str]]:
    with open(filename, "r", encoding="UTF-8") as file:
        raw = json.load(file)

    results = {}
    for epoch_key, inner in raw.items():
        queries = []
        for q in inner["athena_result"]:
            queries.append(q["sql"])
        results[epoch_key] = queries

    return results


def main():
    data1 = process_file("telemetry_workload.json")
    data2 = process_file("telemetry_workload_100g.json")

    combined = {**data1, **data2}
    with open("telemetry_queries.json", "w", encoding="UTF-8") as file:
        json.dump(combined, file, indent=2)


if __name__ == "__main__":
    main()
