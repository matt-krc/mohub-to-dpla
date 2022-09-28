from oai import OAI
import argparse
import utils
import requests
import institutions
import sys


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
    parser.add_argument('--upload', '-u', default=False, help="If set, uploads compiled file to S3.", action="store_true")
    parser.add_argument('--count', default=False, help="Returns total amount of records from most recent ingest.", action="store_true")
    args = parser.parse_args()

    # If we only want to compile existing data files into combined file, set this argument.
    if args.compile_only:
        utils.compile(args.upload)
        return True

    if args.count:
        utils.return_count()
        return True

    institutions_data = institutions.get()

    for institution in institutions_data:
        if args.institutions:
            if institution.id not in args.institutions:
                continue

        if institution.id == 'mhm':
            # Missouri History Museum provides a data dump feed instead of an OAI feed
            data = requests.get(institution.url).json()
            metadata = data['records']
            utils.write_file("files/institutions/", metadata, institution.id, institution.name, 0)

        if institution.id == 'isu':
            # TODO: Onboard ISU
            continue

        else:
            # Create OAI object based on input data
            feed = OAI(institution)

            # In order not to crawl redundantly, by default we skip crawls from the past 24 hours
            if utils.crawled_recently(institution.id) and not args.ignore_time:
                print("{} has been crawled in less than 24 hours, continuing.".format(institution.id))
                continue

            # Crawl the feed and write output to JSON
            feed.crawl()

    if args.csv:
        utils.generate_csvs()

    if not args.institutions:
        # If crawling everything, generate compile output file and report
        utils.compile(args.upload)


if __name__ == "__main__":
    main()
