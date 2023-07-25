import argparse
import pyodbc
import json


TABLES = [
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cstr", type=str, required=True)
    args = parser.parse_args()

    conn = pyodbc.connect(args.cstr)
    cursor = conn.cursor()

    table_max_ids = {}

    for table in TABLES:
        cursor.execute("SELECT MAX(id) FROM {}".format(table))
        row = cursor.fetchone()
        table_max_ids[table] = row[0]

    print(json.dumps(table_max_ids, indent=2))


if __name__ == "__main__":
    main()
