import argparse
import logging
from pathlib import Path
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument("sql_file", help="Name of file in the transform/sql folder")


if __name__ == "__main__":
    args = parser.parse_args()

    sql_file = Path(__file__).parent.parent / f'transform/sql/{filename}.sql'

    command = f"psql $HEROKU_POSTGRESQL_URL -f {sql_file}"

    process = subprocess.run(command, shell=True, check=True, capture_output=True)

    logging.info(process.stdout)