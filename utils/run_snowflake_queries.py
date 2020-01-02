#!/usr/bin/python

import argparse
import os
import sys

from extract.utils import snowflake_engine_factory, execute_query


parser = argparse.ArgumentParser()
parser.add_argument("role", help="The role to run the queries for")
parser.add_argument("schema", help="Default schema to use for queries")


if __name__ == "__main__":
    args = parser.parse_args()

    engine = snowflake_engine_factory(os.environ.copy(), args.role, args.schema)

    for line in sys.stdin:
       line = line.rstrip("\n")
       execute_query(engine, line)
