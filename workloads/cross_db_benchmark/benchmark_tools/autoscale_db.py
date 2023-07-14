import os
import numpy as np
import pandas as pd

from workloads.cross_db_benchmark.benchmark_tools.utils import load_schema_json


def duplicate_data(
    df_table,
    table_name,
    scaled_data_dir,
    factor,
    equivalent_key,
    all_PK_val,
    all_PK_mappings,
):
    # converting string type FKs to int type according to their corresponding PKs
    for key in equivalent_key:
        t_key = key.split(".")[-1]
        assert table_name == key.split(".")[0]
        e_key = equivalent_key[key]
        if e_key in all_PK_mappings:
            col = df_table[t_key].values.astype(str)
            col = np.char.strip(col)
            mapping = all_PK_mappings[e_key]
            PK_values = np.asarray(list(mapping.keys()))
            idx = np.nonzero(PK_values == col[:, None])[1]
            col[np.in1d(col, PK_values)] = np.asarray(list(mapping.values()))[idx]
            df_table[t_key] = col
            df_table[t_key] = pd.to_numeric(df_table[t_key], errors="coerce").astype(
                pd.Int64Dtype()
            )

    factor = int(factor)
    assert factor >= 2
    old_len = len(df_table)
    df_table = pd.concat([df_table] * factor, ignore_index=True)
    assert len(df_table) == old_len * factor

    for key in equivalent_key:
        t_key = key.split(".")[-1]
        e_key = equivalent_key[key]
        if key in all_PK_val:
            # duplicating a primary key
            max_val = all_PK_val[key]
            col = df_table[key.split(".")[-1]].values
            for i in range(1, factor):
                start = old_len * i
                end = old_len * (i + 1)
                col[start:end] += max_val * i
            df_table[t_key] = col
            df_table[t_key] = df_table[t_key].astype(pd.Int64Dtype())

        elif e_key in all_PK_val:
            col = df_table[t_key].values
            added_col_len = len(col[old_len:])
            col[old_len:] += (
                np.random.randint(0, factor, size=added_col_len) * all_PK_val[e_key]
            )
            df_table[t_key] = col
            df_table[t_key] = df_table[t_key].astype(pd.Int64Dtype())
    df_table.to_csv(scaled_data_dir + table_name + ".csv", index=False)


def detect_PK(df_table, t, pkey):
    new_PK_val = dict()
    PK_mappings = dict()
    key_name = t + "." + pkey
    col = df_table[pkey].values
    if col.dtype == int:
        new_PK_val[key_name] = np.nanmax(col) - np.nanmin(col) + 1
    else:
        col = col.astype(str)
        col = np.char.strip(col)
        col_uni = list(np.unique(col))
        val_dict = dict(zip(col_uni, list(np.arange(len(col_uni)) + 1)))
        PK_mappings[key_name] = val_dict
        idx = np.nonzero(np.asarray(list(val_dict.keys())) == col[:, None])[1]
        col[np.in1d(col, np.asarray(col_uni))] = np.asarray(list(val_dict.values()))[
            idx
        ]
        df_table[pkey] = col
        df_table[pkey] = pd.to_numeric(df_table[pkey], errors="coerce").astype(
            pd.Int64Dtype()
        )
        new_PK_val[key_name] = len(
            col_uni
        )  # a random large number that is unlike to appear in the key
    return new_PK_val, PK_mappings


def auto_scale(data_dir, dataset, factor=1):
    schema = load_schema_json(dataset)
    data_path = data_dir + dataset + "/data/"
    scaled_data_dir = data_dir + dataset + "/scaled_data/"
    if not os.path.exists(scaled_data_dir):
        os.mkdir(scaled_data_dir)

    all_data = dict()
    all_PK_val = dict()
    all_keys = dict()
    all_PK_mappings = dict()
    for r in schema.relationships:
        if r[0] not in all_keys:
            all_keys[r[0]] = set()
        all_keys[r[0]].add(r[0] + "." + r[1])
        if r[2] not in all_keys:
            all_keys[r[2]] = set()
        all_keys[r[2]].add(r[2] + "." + r[3])

    for t in all_keys:
        table_dir = os.path.join(data_path, f"{t}.csv")
        assert os.path.exists(table_dir), f"Could not find table csv {table_dir}"
        df_table = pd.read_csv(table_dir, **vars(schema.csv_kwargs))
        all_data[t] = df_table
        if hasattr(schema.primary_key, t):
            new_PK_val, PK_mappings = detect_PK(
                df_table, t, getattr(schema.primary_key, t)
            )
            all_PK_val.update(new_PK_val)
            all_PK_mappings.update(PK_mappings)

    for t in all_keys:
        equivalent_key = dict()
        for r in schema.relationships:
            if t == r[0]:
                key = t + "." + r[1]
                equivalent_key[key] = r[2] + "." + r[3]
            if t == r[2]:
                key = t + "." + r[3]
                equivalent_key[key] = r[0] + "." + r[1]
        duplicate_data(
            all_data[t],
            t,
            scaled_data_dir,
            factor,
            equivalent_key,
            all_PK_val,
            all_PK_mappings,
        )
