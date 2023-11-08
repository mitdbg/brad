import numpy as np


def formate_time_str(m, d):
    if m < 10:
        m = '0' + str(m)
    else:
        m = str(m)
    if d < 10:
        d = '0' + str(d)
    else:
        d = str(d)
    return f"2023-{m}-{d} 00:00:00.000"


def generate_workload(query_templates=None, num_queries_per_template=10, seed=0, save_file=None):
    np.random.seed(seed)
    if query_templates is None or len(query_templates) == 0:
        query_templates = ["SELECT COUNT(*) FROM movie_telemetry WHERE timestamp > '{start_time}' AND timestamp < '{end_time}';",
                           "SELECT COUNT(*) FROM movie_telemetry WHERE movie_id > {movie_start_id} AND movie_id < {movie_end_id};",
                           "SELECT COUNT(*) FROM movie_telemetry WHERE event_id > {event_start_id} AND event_id < {event_end_id};",
                           "SELECT COUNT(*) FROM movie_telemetry WHERE timestamp > '{start_time}' AND timestamp < '{end_time}' AND event_id > {event_start_id} AND event_id < {event_end_id} AND movie_id > {movie_start_id} AND movie_id < {movie_end_id};"]

    MIN_MOVIE_ID = 185208
    MAX_MOVIE_ID = 2440851
    MIN_EVENT_ID = 0
    MAX_EVENT_ID = 20

    all_query_sql = []
    for i in range(num_queries_per_template):
        m = np.random.choice(12, size=2) + 1
        d = np.random.choice(28, size=2) + 1
        if m[0] == m[1]:
            if d[0] == d[1]:
                if d[0] == 0:
                    start_time = formate_time_str(m[0], 0)
                    end_time = formate_time_str(m[0], 1)
                else:
                    start_time = formate_time_str(m[0], d[0]-1)
                    end_time = formate_time_str(m[0], d[0])
            else:
                start_time = formate_time_str(m[0], np.min(d))
                end_time = formate_time_str(m[0], np.max(d))
        else:
            if m[0] < m[1]:
                start_time = formate_time_str(m[0], d[0])
                end_time = formate_time_str(m[1], d[1])
            else:
                start_time = formate_time_str(m[1], d[1])
                end_time = formate_time_str(m[0], d[0])

        movie_id = np.random.choice(MAX_MOVIE_ID - MIN_MOVIE_ID, size=2, replace=False)
        movie_start_id = MIN_MOVIE_ID + np.min(movie_id)
        movie_end_id = MIN_MOVIE_ID + np.max(movie_id)

        event_id = np.random.choice(MAX_EVENT_ID - MIN_EVENT_ID, size=2, replace=False)
        event_start_id = MIN_EVENT_ID + np.min(event_id)
        event_end_id = MIN_EVENT_ID + np.max(event_id)

        query_args = dict()
        for template in query_templates:
            if "'{start_time}'" in template:
                query_args['start_time'] = start_time
            if "'{end_time}'" in template:
                query_args['end_time'] = end_time
            if "{movie_start_id}" in template:
                query_args['movie_start_id'] = movie_start_id
            if "{movie_end_id}" in template:
                query_args['movie_end_id'] = movie_end_id
            if "{event_start_id}" in template:
                query_args['event_start_id'] = event_start_id
            if "{event_end_id}" in template:
                query_args['event_end_id'] = event_end_id
            sql = template.format(**query_args)
            all_query_sql.append(sql)

    if save_file:
        with open(save_file, "w+") as f:
            for sql in all_query_sql:
                f.write(sql + "\n")
    return all_query_sql

