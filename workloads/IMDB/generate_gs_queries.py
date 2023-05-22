import json
import numpy as np

def query_type0():
    dist = np.random.uniform(0, 1000)
    query = f"""
    SELECT c.id 
    FROM cinemas AS c 
    JOIN hospitals AS h 
    ON ST_Distance(ST_Point(h.x_coord,h.y_coord), ST_Point(c.x_coord,c.y_coord)) < {dist};
    """
    return query

def query_type1():
    dist = np.random.uniform(0, 1000)
    query = f"""
    SELECT ST_Distance(ST_Point(h.x_coord,h.y_coord), ST_Point(c.x_coord,c.y_coord)), h.id, c.id 
    FROM homes AS h 
    JOIN cinemas AS c ON ST_Distance(ST_Point(h.x_coord,h.y_coord), ST_Point(c.x_coord,c.y_coord)) < {dist};
    """
    return query

def query_type2():
    dist = np.random.uniform(0, 1000)
    query = f"""
    SELECT c.id, COUNT(*) 
    FROM cinemas AS c 
    JOIN homes AS h ON ST_Distance(ST_Point(h.x_coord,h.y_coord), ST_Point(c.x_coord,c.y_coord)) < {dist} 
    GROUP BY c.id 
    ORDER BY c.id;
    """
    return query

def query_type3():
    dist = np.random.uniform(0, 1000)
    dist_cin = dist*np.random.uniform(0.5, 1.1)

    query = f"""
    WITH neighbours(h1, h2, h1_x, h1_y, h2_x, h2_y) AS (
        SELECT h1.id, h2.id, h1.x_coord, h1.y_coord, h2.x_coord, h2.y_coord 
        FROM homes AS h1 
        JOIN homes AS h2 ON ST_Distance(ST_Point(h1.x_coord,h1.y_coord), ST_Point(h2.x_coord,h2.y_coord)) < {dist} AND h1.id != h2.id
    ),
    distsum(h1, h2, c, d) AS (
        SELECT h1, h2, c.id, ST_Distance(ST_Point(n.h1_x,n.h1_y), ST_Point(c.x_coord,c.y_coord)) + ST_Distance(ST_Point(n.h2_x,n.h2_y), ST_Point(c.x_coord,c.y_coord)) 
        FROM neighbours AS n 
        JOIN cinemas AS c ON ST_Distance(ST_Point(n.h1_x,n.h1_y), ST_Point(c.x_coord,c.y_coord)) + ST_Distance(ST_Point(n.h2_x,n.h2_y), ST_Point(c.x_coord,c.y_coord)) < {dist_cin}
    )
    SELECT h1, h2, MIN(d) FROM distsum GROUP BY h1, h2;
    """

    return query

def query_type4():
    dist = np.random.uniform(0, 600)
    cutoff = np.random.randint(80)

    query = f"""
    WITH homecinema(h, c) AS (
        SELECT h.id, c.id 
        FROM homes AS h 
        JOIN cinemas AS c ON ST_Distance(ST_Point(h.x_coord,h.y_coord), ST_Point(c.x_coord,c.y_coord)) < {dist}
    ),
    homecinemacount(h, c) AS (
        SELECT h, COUNT(*) 
        FROM homecinema 
        GROUP BY h
    )
    SELECT h, c 
    FROM homecinemacount 
    WHERE c > {cutoff} 
    ORDER BY c;
    """

    return query

def query_type5(area_categs=3):
    places_tables = ["homes", "cinemas", "hospitals"]
    t = places_tables[np.random.randint((len(places_tables)))]
    d = np.random.randint(area_categs)
    query = f"""
    SELECT p.id, p.x_coord, p.y_coord 
    FROM {t} AS p 
    JOIN area_desc AS a ON ST_Within(ST_Point(p.x_coord, p.y_coord),  ST_GeometryFromText(a.polygon)) 
    WHERE a.descr = {d};
    """
    return query

def generate_gs_queries(num_queries: list[int]) -> list[str]:
    # query generator functions
    query_generators = [query_type0,
                        query_type1,
                        query_type2,
                        query_type3,
                        query_type4,
                        query_type5]

    # init
    assert len(num_queries) == len(query_generators)
    num_queries = np.array(num_queries)
    remain_queries = np.sum(num_queries)
    all_queries = []

    # generate queries
    while remain_queries > 0:
        query_type = np.random.choice(list(range(len(num_queries))), p=num_queries/remain_queries)
        query_str = query_generators[query_type]()
        all_queries.append(query_str)

        remain_queries -= 1
        num_queries[query_type] -= 1

    return all_queries