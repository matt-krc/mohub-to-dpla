import json
import os
from datetime import datetime
from oai import OAI
import argparse
import utils
import requests
import institutions

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

    data = institutions.get()

    report = {
        "institutions": {}
    }
    total = 0

    for row in data:
        url = row['url']
        institution = row['institution']

        if args.institutions:
            if row['id'] not in args.institutions:
                continue

        if row['id'] == 'mhm':
            url = row['url']
            data = requests.get(url).json()
            metadata = data['records']

        else:
            # Create OAI object based on input data
            feed = OAI(row)

            # In order not to crawl redundantly, by default we skip crawls that have already taken place in the past 24 hours
            if utils.crawled_recently(row['id']) and not args.ignore_time:
                print("{} has been crawled in less than 24 hours, continuing.".format(institution))
                continue

            print(institution)
            print(url)

            # Some institutions have opted to provide a data dump rather than an OAI feed.
            metadata, skipped = feed.crawl()

            report['institutions'][row['id']] = {
                "total": len(metadata),
                "skipped": skipped
            }
        total += len(metadata)

        utils.write_file("files/institutions/", metadata, row["id"])

    report['total'] = total
    report['crawled'] = '*' if not args.institutions else args.institutions
    outp = "files/reports/report_{}.json".format(datetime.now().strftime("%Y%m%d%H%M%S"))
    with open(outp, "w") as outf:
        json.dump(report, outf, indent=4)
    print("Report saved to {}".format(outp))

    if args.csv:
        if args.institutions:
            for i in args.institutions:
                utils.generate_csvs(i)
        else:
            utils.generate_csvs()
    utils.compile()


if __name__ == "__main__":
    main()