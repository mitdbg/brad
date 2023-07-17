# We adapted the legacy from from https://github.com/DataManagementLab/zero-shot-cost-estimation
import json
import os
from types import SimpleNamespace


def dumper(obj):
    try:
        return obj.toJSON()
    except:
        return obj.__dict__


def load_schema_json(dataset):
    schema_path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "schema.json"
    )
    assert os.path.exists(schema_path), f"Could not find schema.json ({schema_path})"
    return load_json(schema_path)


def save_schema_json(dataset, schema):
    schema_path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "schema.json"
    )
    assert os.path.exists(schema_path), f"Could not find schema.json ({schema_path})"
    with open(schema_path, "w") as f:
        json.dump(schema, f, default=dumper)


def load_column_statistics(dataset, namespace=True):
    path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "column_statistics.json"
    )
    assert os.path.exists(path), f"Could not find file ({path})"
    return load_json(path, namespace=namespace)


def load_string_statistics(dataset, namespace=True):
    path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "string_statistics.json"
    )
    assert os.path.exists(path), f"Could not find file ({path})"
    return load_json(path, namespace=namespace)


def load_json(path, namespace=True):
    with open(path) as json_file:
        if namespace:
            json_obj = json.load(json_file, object_hook=lambda d: SimpleNamespace(**d))
        else:
            json_obj = json.load(json_file)
    return json_obj


def load_schema_sql(dataset, sql_filename):
    sql_path = os.path.join(
        "workloads/cross_db_benchmark/datasets/", dataset, "schema_sql", sql_filename
    )
    assert os.path.exists(sql_path), f"Could not find schema.sql ({sql_path})"
    with open(sql_path, "r") as file:
        data = file.read().replace("\n", "")
    return data
