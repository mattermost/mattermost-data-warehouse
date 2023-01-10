#!/usr/bin/python

import argparse
import os

from extract.utils import snowflake_engine_factory

parser = argparse.ArgumentParser()
parser.add_argument("sql_file", help="The SQL file to run on Snowflake")
parser.add_argument("role", default="TRANSFORMER", help="The role to run the queries for")
parser.add_argument("schema", help="Default schema to use for queries")


def run_queries(args, base_path="transform/sql/snowflake"):
    """
    Reads queries from provided file one by one.

    :param args: parsed arguments using argparse. Must contain arguments `sql_file`, `role` and `schema`.
    :param base_path: name of the file (without a `.sql` extension) to load from `base_path` and run it.
    """
    engine = snowflake_engine_factory(os.environ.copy(), args.role, args.schema)

    with open(f"{base_path}/{args.sql_file}.sql") as f:
        content = f.read()
        content = content.replace("\;", "SEMICOLONTEMPFIX")  # noqa: W605
        queries = content.split(";")
        with engine.begin() as conn:
            [conn.execute(query.replace("SEMICOLONTEMPFIX", ";")) for query in queries]


if __name__ == "__main__":
    args = parser.parse_args()
    run_queries(args)
