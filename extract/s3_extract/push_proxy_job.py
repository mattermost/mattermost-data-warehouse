import argparse
import sys

from extract.s3_extract.stage_import import push_proxy_import

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--log-type", help="Type to import, can be US, TEST, or DE",
                    action="store_true")
parser.add_argument("-d", "--date", help="Date to execute import for",
                    action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    push_proxy_import(args.log_type, args.date)