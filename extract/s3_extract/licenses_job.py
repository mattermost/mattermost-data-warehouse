import argparse

from extract.s3_extract.stage_import import licenses_import

parser = argparse.ArgumentParser()
parser.add_argument("date", help="Date to execute import for")

if __name__ == "__main__":
    args = parser.parse_args()

    licenses_import(args.date)
