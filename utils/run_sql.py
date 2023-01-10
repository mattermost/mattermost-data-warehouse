import argparse
import logging
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("sql_file", help="Name of file in the transform/sql folder")


if __name__ == "__main__":
    args = parser.parse_args()

    sql_file = f"transform/sql/{args.sql_file}.sql"

    command = f"psql -v ON_ERROR_STOP=1 $HEROKU_POSTGRESQL_URL -f {sql_file}"

    process = subprocess.run(command, shell=True, capture_output=True, check=True)

    logging.info(process.stdout)
