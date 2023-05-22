import numpy as np
import pyodbc
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.server.engine_connections import EngineConnections

def gen_place(grid_size=1000):
    x = np.random.uniform(grid_size)
    y = np.random.uniform(grid_size)
    return f"{x},{y}"

def gen_area(side_len=50, boundary=1000):
    x = np.random.uniform(boundary-side_len)
    y = np.random.uniform(boundary-side_len)
    descr = np.random.randint(3)
    return f"{descr},'POLYGON(({x} {y},{x+side_len} {y},{x+side_len} {y+side_len},{x} {y+side_len},{x} {y}))'"

def generate_gs_data(num_rows: list[int], start_ids: list[int]) -> list[str]:
    # init
    assert len(num_rows) == len(start_ids) == 4
    query_temp = "INSERT INTO {table} (id,{column_clause}) VALUES ({id},{select_clause})"
    all_queries = []

    # generate cinemas
    # TODO: start ids needed because nothing like AUTO_INCREMENT in athena.
    # check if we can generate iceberg tables not using athena (also slow)
    for i in range(num_rows[0]):
        query = query_temp.format(id=start_ids[0]+i, table="cinemas", column_clause="x_coord,y_coord", select_clause=gen_place())
        all_queries.append(query)

    # generate hospital
    for i in range(num_rows[1]):
        query = query_temp.format(id=start_ids[1]+i, table="hospitals", column_clause="x_coord,y_coord", select_clause=gen_place())
        all_queries.append(query)

    # generate homes
    for i in range(num_rows[2]):
        query = query_temp.format(id=start_ids[2]+i, table="homes", column_clause="x_coord,y_coord", select_clause=gen_place())
        all_queries.append(query)

    # generate area_desc
    for i in range(num_rows[3]):
        query = query_temp.format(id=start_ids[3]+i, table="area_desc", column_clause="descr,polygon", select_clause=gen_area())
        all_queries.append(query)

    return all_queries

if __name__ == "__main__":
    start_ids = [0, 0, 0, 0] # TODO
    queries = generate_gs_data([0, 0, 0, 5], start_ids)
    config = ConfigFile("../../config/config_ferdiko.yml")

    # only connect to athena
    conn = pyodbc.connect(config.get_odbc_connection_string(Engine.Athena, None))

    # connect using brad helpers
    # conn = EngineConnections.connect_sync(config, autocommit=True).get_connection(Engine.Athena)

    # insert
    cursor = conn.cursor()
    for q in queries:
        print(q)
        cursor.execute(q)
    conn.close()