import json
import time
import os
from glob import glob
from datetime import datetime, timedelta
from oai import OAI
import sys
import argparse
import utils

def write_files(out, inst_id):
    outpath = f"./files/institutions/{inst_id}.json"
    with open(outpath, "w") as outf:
        json.dump(out, outf, indent=4)

    print(f"\n{len(out)} records written to {inst_id}.json")

def create_csvs(institution="*"):
    json_files = glob("./files/institutions/{}.json".format(institution))
    for file in json_files:
        print(file)
        with open(file, "r") as inf:
            data = json.load(inf)
        OAI().write_csv(data, file.replace(".json",".csv"))

def compile():
    json_files = glob("./files/institutions/*.json")
    print("Compiling...")
    out = []
    for file in json_files:
        print(file)
        with open(file, "r") as inf:
            data = json.load(inf)
        out.extend(data)
        inf.close()
    datestr = datetime.now().strftime("%Y_%m_%d")
    outfn = "./files/ingests/mohub_ingest_{}.json".format(datestr)
    with open(outfn, "w") as outf:
        json.dump(out, outf, indent=4)
    # finish by writing to jsonl, as DPLA prefers
    with open(outfn + "l", "w") as outf:
        for line in out:
            json.dump(line, outf)
            outf.write('\n')
    print("Total: {}".format(len(out)))
    print("Wrote ingest file to {}".format(outfn))

def get_previous(institution, outfn):
    with open("./files/mohub_dpla.json", "r") as inf:
        data = json.load(inf)
    filtered = [d for d in data if d['dataProvider'] == institution]
    with open("./files/institutions/{}".format(outfn), "w") as outf:
        json.dump(filtered, outf, indent=4)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--institutions', '-i', nargs='*',
                        help="If argument is set, specifies specific institutions to crawl.", required=False)
    parser.add_argument('--ignore_time', '-g', default=False,
                        action="store_true", help="If set, ignores whether data already harvested recently.")
    parser.add_argument('--csv', '-csv', default=False,
                        action="store_true", help="If set, generates accompanying CSV files.")
    parser.add_argument('--compile', '-c', default=False,
                        help="If set, compiles all data files into one.", action="store_true")
    parser.add_argument('--compile_only', '-o', default=False,
                        help="If set, script doesn't run a crawl, only compiles all data files into one.", action="store_true")
    args = parser.parse_args()

    # If we only want to compile existing data files into combined file, set this argument.
    if args.compile_only:
        compile()
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
        
    if not os.path.isdir('./files/ingests'):
        os.mkdir('./files/ingests')

    if not os.path.isdir('./files/institutions'):
        os.mkdir('./files/institutions')

    for row in data:
        out = []
        url = row['url']
        institution = row['institution']
        metadata_prefix = row['metadata_prefix']

        if args.institutions:
            if row['id'] not in args.institutions:
                continue

        # In order not to crawl redundantly, by default we skip crawls that have already taken place in the past 24 hours
        if not args.ignore_time and os.path.exists(f"./files/institutions/{row['id']}.json"):
            now = datetime.now()
            if now-timedelta(hours=24) <= datetime.fromtimestamp(os.path.getmtime(f"./files/institutions/{row['id']}.json")) <= now:
                print(f"{institution} has been crawled less than 24 hours ago. Continuing")
                continue

        feed = OAI(row)
        print(institution)
        print(url)

        # Some institutions have opted to provide a data dump rather than an OAI feed.
        if metadata_prefix == 'data_dump':
            metadata = utils.get_datadump(url)
        else:
            metadata = feed.crawl()

        out.extend(metadata)
        write_files(out, row["id"])
    if args.csv:
        if args.institutions:
            for i in args.institutions:
                create_csvs(i)
        else:
            create_csvs()
    if args.compile:
        compile()


if __name__ == "__main__":
    main()