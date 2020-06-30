#!/usr/bin/python

import argparse
import os
import sys

from extract.utils import snowflake_engine_factory, execute_query


parser = argparse.ArgumentParser()
parser.add_argument("sql_file", help="The SQL file to run on Snowflake")
parser.add_argument(
    "role", default="TRANSFORMER", help="The role to run the queries for"
)
parser.add_argument("schema", help="Default schema to use for queries")


if __name__ == "__main__":
    args = parser.parse_args()

    engine = snowflake_engine_factory(os.environ.copy(), args.role, args.schema)

    with open(f"transform/sql/snowflake/{args.sql_file}.sql") as f:
        content = f.read()
        queries = content.split(";")
        with engine.begin() as conn:
            [conn.execute(query) for query in queries]
