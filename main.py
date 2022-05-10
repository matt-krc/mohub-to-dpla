import json
import os
from datetime import datetime
from oai import OAI
import argparse
import utils
import requests
import institutions
import sys

if not os.path.isdir('files/ingests'):
    os.mkdir('files/ingests')

if not os.path.isdir('files/institutions'):
    os.mkdir('files/institutions')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--institutions', '-i', nargs='*',
                        help="If argument is set, specifies specific institutions to crawl.", required=False)
    parser.add_argument('--ignore_time', '-ig', default=False,
                        action="store_true", help="If set, ignores whether data has been harvested in the past 24 hours already.")
    parser.add_argument('--csv', '-csv', default=False,
                        action="store_true", help="If set, converts JSON output to CSV.")
    parser.add_argument('--compile_only', '-co', default=False,
                        help="If set, script doesn't run a crawl, only compiles all data files into one.", action="store_true")
    parser.add_argument('--count', default=False, help="Returns total amount of records from most recent ingest.", action="store_true")
    args = parser.parse_args()

    # If we only want to compile existing data files into combined file, set this argument.
    if args.compile_only:
        utils.compile()
        return True

    if args.count:
        utils.return_count()
        return True

    institutions_data = institutions.get()

    total = 0

    for institution in institutions_data:
        if args.institutions:
            if institution.id not in args.institutions:
                continue

        if institution.id == 'mhm':
            # Missouri History Museum provides a data dump feed instead of an OAI feed
            data = requests.get(institution.url).json()
            metadata = data['records']

        else:
            # Create OAI object based on input data
            feed = OAI(institution)

            # In order not to crawl redundantly, by default we skip crawls from the past 24 hours
            if utils.crawled_recently(institution.id) and not args.ignore_time:
                print("{} has been crawled in less than 24 hours, continuing.".format(institution))
                continue

            print(institution.name)
            print(institution.url)

            metadata, skipped = feed.crawl()

        utils.write_file("files/institutions/", metadata, institution.id, institution.name, skipped)

    if args.csv:
        if args.institutions:
            for i in args.institutions:
                utils.generate_csvs(i)
        else:
            utils.generate_csvs()

    if not args.institutions:
        # If crawling everything, generate compile output file and report
        utils.compile()

if __name__ == "__main__":
    main()
