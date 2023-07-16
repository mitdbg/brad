import os
import copy
import numpy as np
import pandas as pd

from workloads.cross_db_benchmark.benchmark_tools.utils import load_schema_json


def duplicate_data(
        schema,
        source_table_dir,
        table_name,
        scaled_data_dir,
        factor,
        equivalent_key,
        all_PK_val,
        all_PK_mappings,
        PK_randomness=False
):
    # converting string type FKs to int type according to their corresponding PKs
    df_table = pd.read_csv(source_table_dir, sep="|", header=0, escapechar="\\")

    target_file = os.path.join(scaled_data_dir, table_name + "_0.csv")
    df_table.to_csv(target_file, index=False, sep="|", escapechar="\\", header=True)

    for key in equivalent_key:
        t_key = key.split(".")[-1]
        assert table_name == key.split(".")[0]
        e_key = equivalent_key[key]
        if e_key in all_PK_mappings:
            # dealing with non-int type keys
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
    old_key_data = dict()
    for key in equivalent_key:
        t_key = key.split(".")[-1]
        old_key_data[t_key] = copy.deepcopy(df_table[t_key].values)

    for i in range(1, factor):
        # save them as seperate file and combine them later to for memory efficiency
        target_file = os.path.join(scaled_data_dir, table_name + f"_{i}.csv")
        for key in equivalent_key:
            t_key = key.split(".")[-1]
            e_key = equivalent_key[key]
            if key in all_PK_val:
                # if a key is primary key, we add it with a offset
                offset = all_PK_val[key]
                col = old_key_data[t_key] + offset * i
                df_table[t_key] = col
                df_table[t_key] = df_table[t_key].astype(pd.Int64Dtype())
            elif e_key in all_PK_val:
                offset = all_PK_val[e_key]
                if PK_randomness:
                    col = old_key_data[t_key] + np.random.randint(0, factor, size=old_len) * offset
                else:
                    col = old_key_data[t_key] + offset * i
                df_table[t_key] = col
                df_table[t_key] = df_table[t_key].astype(pd.Int64Dtype())
            else:
                continue
        df_table.to_csv(target_file, index=False, sep="|", escapechar="\\", header=False)

    target_file = os.path.join(scaled_data_dir, table_name + ".csv")
    with open(target_file, 'w') as write_f:
        for i in range(0, factor):
            # panda will treat int column with Nan as float, need to remove ".0" for those value to avoid
            # errors in loading
            temp_target_file = os.path.join(scaled_data_dir, table_name + f"_{i}.csv")
            with open(temp_target_file, "r") as f:
                text = f.read()
            text = text.strip()
            text = text.replace(".0|", "|")
            if i != 0:
                text = "\n" + text
            write_f.write(text)
            del text
            os.remove(temp_target_file)


def detect_PK(df_table, t, pkey):
    new_PK_val = dict()
    PK_mappings = dict()
    key_name = t + "." + pkey
    col = df_table[pkey].values
    if col.dtype == int:
        new_PK_val[key_name] = np.nanmax(col) + 1
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


def auto_scale(data_dir, target_dir, dataset, factor=2, PK_randomness=False):
    schema = load_schema_json(dataset)
    data_path = data_dir
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)

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
        if hasattr(schema.primary_key, t):
            df_table = pd.read_csv(table_dir, **vars(schema.csv_kwargs))
            new_PK_val, PK_mappings = detect_PK(
                df_table, t, getattr(schema.primary_key, t)
            )
            all_PK_val.update(new_PK_val)
            all_PK_mappings.update(PK_mappings)
            del df_table

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
            schema,
            os.path.join(data_path, f"{t}.csv"),
            t,
            target_dir,
            factor,
            equivalent_key,
            all_PK_val,
            all_PK_mappings,
            PK_randomness
        )
