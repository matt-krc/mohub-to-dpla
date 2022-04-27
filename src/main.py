import json
import os
from datetime import datetime
from oai import OAI
import argparse
import utils
<<<<<<< Updated upstream:main.py
=======
import requests
>>>>>>> Stashed changes:src/main.py

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--institutions', '-i', nargs='*',
                        help="If argument is set, specifies specific institutions to crawl.", required=False)
    parser.add_argument('--ignore_time', '-ig', default=False,
                        action="store_true", help="If set, ignores whether data already harvested recently.")
    parser.add_argument('--csv', '-csv', default=False,
                        action="store_true", help="If set, generates accompanying CSV files.")
    parser.add_argument('--compile', '-c', default=False,
                        help="If set, compiles all data files into one.", action="store_true")
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
    """
    The main input file is a path to a JSON file containing an array of OAI endpoint URLs, along with additional institutional metadata:
    {
        "institution": // the name of the institution,
        "id": // internal id for institution, used in the mapping functions,
        "@id_prefix": // a prefix for the item @id field in the output metadata,
        "url": // url to the root OAI endpoint, or data dump,
        "metadata_prefix": // metadata prefix for OAI feed, to be used in constructing the OAI query. If set to 'data_dump', URL assumed to be downloaded as-is,
        "include": // array listing collection names to be included in crawl. If set, only collections listed will be included, otherwise all collections assumed to be included,
        "exclude": // array listing collection names to be excluded in crawl. If set, all but listed collections will be excluded
    }
    """
    infile = "./files/mohub_oai.json"
    with open(infile, "r") as inf:
        data = json.load(inf)
        
    if not os.path.isdir('../files/ingests'):
        os.mkdir('../files/ingests')

    if not os.path.isdir('../files/institutions'):
        os.mkdir('../files/institutions')

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
    if args.compile:
        utils.compile()


if __name__ == "__main__":
    main()