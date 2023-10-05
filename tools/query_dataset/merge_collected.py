import argparse
import json


def merge_athena_json_files(input_file1, input_file2, output_file):
    # Read JSON files
    with open(input_file1, "r", encoding="UTF-8") as file1:
        data1 = json.load(file1)
    with open(input_file2, "r", encoding="UTF-8") as file2:
        data2 = json.load(file2)

    # Merge the query_list values
    query_list1 = data1["query_list"]
    query_list2 = data2["query_list"]

    # Update query_index in the second JSON
    last_query_index = query_list1[-1]["query_index"]
    offset = last_query_index + 1
    for query in query_list2:
        query["query_index"] += offset

    # Concatenate the query_lists
    merged_query_list = query_list1 + query_list2

    # Update total_time_secs (if needed)
    data1["total_time_secs"] += data2["total_time_secs"]

    # Create the merged JSON object
    merged_data = {
        "query_list": merged_query_list,
        "run_kwargs": data2["run_kwargs"],
        "total_time_secs": data1["total_time_secs"],
    }

    # Write the merged JSON object to the output file
    with open(output_file, "w", encoding="UTF-8") as outfile:
        json.dump(merged_data, outfile, indent=4)


def merge_aurora_redshift_json_files(input_file1, input_file2, output_file):
    # Read JSON files
    with open(input_file1, "r", encoding="UTF-8") as file1:
        data1 = json.load(file1)
    with open(input_file2, "r", encoding="UTF-8") as file2:
        data2 = json.load(file2)

    # Merge the query_list values
    query_list1 = data1["query_list"]
    query_list2 = data2["query_list"]

    # Update query_index in the second JSON
    last_query_index = query_list1[-1]["query_index"]
    offset = last_query_index + 1
    for query in query_list2:
        query["query_index"] += offset

    # Concatenate the query_lists
    merged_query_list = query_list1 + query_list2

    # Update total_time_secs (if needed)
    data1["total_time_secs"] += data2["total_time_secs"]

    # Create the merged JSON object
    merged_data = {
        "query_list": merged_query_list,
        "database_stats": data1["database_stats"],
        "run_kwargs": data1["run_kwargs"],
        "total_time_secs": data1["total_time_secs"],
    }

    # Write the merged JSON object to the output file
    with open(output_file, "w", encoding="UTF-8") as outfile:
        json.dump(merged_data, outfile, indent=4)


def main():
    parser = argparse.ArgumentParser(
        description="Merge two sets of raw collected data."
    )
    parser.add_argument("--engine", type=str, help="Engine used for the data")
    parser.add_argument("--file1", type=str, help="Path to the first input JSON file")
    parser.add_argument("--file2", type=str, help="Path to the second input JSON file")
    parser.add_argument(
        "--output", type=str, help="Path to the output merged JSON file"
    )
    args = parser.parse_args()

    if args.engine == "athena":
        merge_athena_json_files(args.file1, args.file2, args.output)
    elif args.engine == "aurora" or args.engine == "redshift":
        merge_aurora_redshift_json_files(args.file1, args.file2, args.output)


if __name__ == "__main__":
    main()
