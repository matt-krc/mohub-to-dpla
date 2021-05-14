import json
import time
import os
from glob import glob
from datetime import datetime, timedelta
from oai import OAI
import sys
import argparse


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
    print("Total: {}".format(len(out)))
    print("Wrote ingest file to {}".format(outfn))

def get_previous(institution, outfn):
    with open("./files/mohub_dpla.json", "r") as inf:
        data = json.load(inf)
    filtered = [d for d in data if d['dataProvider'] == institution]
    with open("./files/institutions/{}".format(outfn), "w") as outf:
        json.dump(filtered, outf, indent=4)


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--institutions', '-i', nargs='*',
                        help="If argument is set, specifies specific institutions to parse", required=False)
    parser.add_argument('--ignore_time', '-ig', default=False,
                        action="store_true", help="If set, ignores whether data already harvested recently")
    parser.add_argument('--no_csv', '-ncsv', default=False,
                        action="store_true", help="If set, doesn't generate CSV files.")
    parser.add_argument('--compile', '-c', default=False,
                        help="If set, compiles all data files into one.", action="store_true")
    parser.add_argument('--compile_only', '-co', default=False,
                        help="If set, script doesn't run a crawl, only compiles all data files into one.", action="store_true")
    args = parser.parse_args()

    # infile is a path to a JSON file containing a list of OAI endpoint URLs, along with additional institutional metadata
    infile = './files/mohub_oai.json'

    if args.compile_only:
        compile()
        return True

    with open(infile, "r") as inf:
        data = json.load(inf)

    for row in data:
        out = []
        url = row['url']
        institution = row['institution']

        if args.institutions:
            if row["id"] not in args.institutions:
                continue

        if not args.ignore_time:
            now = datetime.now()
            if now-timedelta(hours=24) <= datetime.fromtimestamp(os.path.getmtime(f"./files/institutions/{row['id']}.json")) <= now:
                print(f"{institution} has been crawled less than 24 hours ago. Continuing")
                continue

        if not url:
            print("No OAI feed for {}. Using previous ingest data.".format(institution))
            get_previous(institution, row["id"] + ".json")
            continue

        # metadata_prefix = OAI().get_metadata_prefix(url)
        metadata_prefix = row["schema"]
        print(institution)
        print(url)
        if not metadata_prefix or not institution:
            print("A metadata prefix or institution name couldn't be established.")

        if row["include"]:
            for include_set in row["include"]:
                metadata = OAI().harvest(url, metadata_prefix, institution, row["id"], row["exclude"], include_set)
                out.extend(metadata)
            write_files(out, row["id"])
            continue

        metadata = OAI().harvest(url, metadata_prefix, institution, row["id"], row["exclude"])
        out.extend(metadata)
        write_files(out, row["id"])
    if not args.no_csv:
        if args.institutions:
            for i in args.institutions:
                create_csvs(i)
        else:
            create_csvs()
    if args.compile:
        compile()


if __name__ == "__main__":
    main()