import argparse
import logging
from pathlib import Path
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument("sql_file", help="Name of file in the transform/sql folder")


if __name__ == "__main__":
    args = parser.parse_args()

    sql_file = f'transform/sql/{args.sql_file}.sql'

    command = f"psql $HEROKU_POSTGRESQL_URL -f {sql_file}"

    process = subprocess.run(command, shell=True, capture_output=True)

    if process.returncode != 0:
        logging.error(process.stderr)
        raise subprocess.CalledProcessError(process.returncode, process.cmd, process.output)

    logging.info(process.stdout)