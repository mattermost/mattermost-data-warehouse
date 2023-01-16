import argparse

from extract.s3_extract.stage_import import push_proxy_import

parser = argparse.ArgumentParser()
parser.add_argument("log_type", help="Type to import, can be US, TEST, or DE")
parser.add_argument("date", help="Date to execute import for")

if __name__ == "__main__":
    args = parser.parse_args()

    push_proxy_import(args.log_type, args.date)
