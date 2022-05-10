from dateutil import parser
import pandas as pd
import requests
import json
from glob import glob
from datetime import datetime, timedelta
import os
import institutions
from iso639 import languages

DATA_DIR = './files/institutions'
REPORTS_DIR = './files/reports'


def get_data_files():
    institutions_data = institutions.get()
    files = []
    for institution in institutions_data:
        files.extend(glob(f"{DATA_DIR}/{institution.id}.json"))
    return files


def compile():
    """
    Compiles all crawled files into one JSONL file to send off to DPLA

    :return:
    """
    # TODO: Upload compiled file to Google Drive directly
    json_files = get_data_files()
    print("Compiling...")
    datetimestr = datetime.now().strftime("%Y%m%d%H%M%S")
    out = []
    for file in json_files:
        print(file)
        with open(file, "r") as inf:
            data = json.load(inf)
        out.extend(data['records'])
        inf.close()
    outfn = "./files/ingests/mohub_ingest_{}.json".format(datetimestr)
    with open(outfn, "w") as outf:
        json.dump(out, outf, indent=4)
    # finish by writing to jsonl, as DPLA prefers
    with open(outfn + "l", "w") as outf:
        for line in out:
            json.dump(line, outf)
            outf.write('\n')
    write_report(datetimestr)
    print("Total: {}".format(len(out)))
    print("Wrote ingest file to {}".format(outfn))


def write_report(datetimestr):
    json_files = get_data_files()
    with open(f"{REPORTS_DIR}/report_{datetimestr}.txt", "w") as outf:
        for file in json_files:
            with open(file, 'r') as inf:
                data = json.load(inf)
            skipped = data['skipped']
            count = data['count']
            name = data['institution']
            outf.write(f"# {name}\n")
            outf.write(f"   - {count} records added\n")
            outf.write(f"   - {skipped} records skipped\n\n")
            inf.close()

def write_file(out_path, metadata, id, name, skipped):
    out_data = {
        "institution": name,
        "count": len(metadata),
        "skipped": skipped,
        "records": metadata
    }
    out_path = out_path if out_path[-1] == '/' else out_path + '/'
    with open("{}{}.json".format(out_path, id), "w") as outf:
        json.dump(out_data, outf, indent=4)

    print(f"\n{len(metadata)} records written to {id}.json")


def generate_csvs():
    """
    Generates CSVs for JSON files, for human-readability purposes

    """
    json_files = get_data_files()
    for file in json_files:
        with open(file, "r") as inf:
            data = json.load(inf)
        write_csv(data['records'], file.replace(".json",".csv"))


def get_metadata(field, metadata):
    """
    Returns metadata based on a field key.
    Subfields are delimited by periods, so we have to recursively loop through metadata fields to get them.

    :param field: a field key, possibly delimited by a period (for subfields)
    :param metadata: metadata object from OAI feed.
    :return: array of values for given field key
    """

    stack = field.split('.')
    _metadata = metadata
    for s in stack:
        if s not in _metadata:
            return False
        _metadata = _metadata[s]
    out = []

    for m in _metadata:
        out.extend([" ".join(a.strip().split()) for a in m.split(";") if a])
    return out


def parse_language(language_list):
    outlist = []
    delimiters = ['/', ',', ';']
    ll = []
    delimited = False
    for language in language_list:
        for d in delimiters:
            if d not in language:
                continue
            delimited = True
            for l in language.split(d):
                if l.strip() not in ll:
                    ll.append(l.strip())
        if not delimited:
            ll.append(language.strip())
        delimited = False
    language_list = ll
    for language in language_list:
        language_found = False
        codes = ['name', 'part3', 'part2b', 'part2t', 'part1']
        for code in codes:
            if code == 'name':
                language = language.capitalize()
            else:
                language = language.lower()
            try:
                if code == 'name':
                    lng = languages.get(name=language)
                elif code == 'part3':
                    lng = languages.get(part3=language)
                elif code == 'part2b':
                    lng = languages.get(part2b=language)
                elif code == 'part2t':
                    lng = languages.get(part2t=language)
                elif code == 'part1':
                    lng = languages.get(part1=language)
                outlist.append({f"iso639_3": lng.part3, "name": lng.name})
                language_found = True
                break
            except (KeyError, SyntaxError) as e:
                continue

    return outlist


def parse_date(datestr):
    """
    Parses a string that may or may not be able to be converted to a datetime object.
    If able to be parsed, returns a string corresponding to the datetime template <Month> <day>, <Year>

    :param datestr: string representing a potential datetime object
    :return: a formatted date string
    """

    try:
        parsed = parser.parse(datestr)
    except ValueError as e:
        print(f"{datestr} could not be parsed as a date. Skipping.")
        return False

    return parsed.strftime("%B %-d, %Y")


def make_list_flat(l):
    """
    Flattens a list of lists, for cleaning purposes

    :param l: a list of lists
    :return: a list of not-list values
    """

    flist = []
    flist.extend([l]) if (type(l) is not list) else [flist.extend(make_list_flat(e)) for e in l]
    return flist


def split_values(row):
    """
    Split fields with semicolons into arrays

    :param row: a row of metadata
    :return: same row of metadata but with certain fields converted to arrays
    """

    fields_to_split = [
        'subject',
        'date',
        'language'
    ]
    for field, value in row.items():
        if field not in fields_to_split:
            continue
        outval = []
        for v in value:
            outval.extend([_v.strip() for _v in v.split(";") if _v != ''])
        row[field] = outval

    return row


def write_csv(data, outpath):
    """
    Helper function to output harvested metadata as a more human-readable CSV file

    :param data: harvested JSON data from an institution
    :param outpath: file path for CSV output
    :return: nothing
    """
    out = []
    for row in data:
        outrow = {}
        outrow["url"] = row["isShownAt"]
        outrow["dataProvider"] = row["dataProvider"]
        outrow["thumbnail"] = row["object"]
        outrow["dplaIdentififer"] = row["@id"]
        for field, val in row["sourceResource"].items():
            if field == "identifier":
                continue
            if type(val) == list:
                if len(val) == 0:
                    outrow[field] = ""
                elif field == "subject":
                    outrow[field] = "|".join([subj["name"] for subj in val])
                elif field == "temporal":
                    outrow["displayDate"] = val[0]["displayDate"]
                elif field == "language":
                    outrow["languageCode"] = "|".join([l["iso639_3"] for l in val])
                    outrow["language"] = "|".join([l["name"] for l in val])
                elif len(val) == 1:
                    outrow[field] = val[0]
                elif len(val) > 1:
                    outrow[field] = "|".join(val)
            else:
                outrow[field] = val
        out.append(outrow)
    outdf = pd.DataFrame(out)
    outdf.to_csv(outpath, index=False)

def get_datadump(url):
    """
    If an institution is providing a data dump instead of an OAI feed, we simply download and return the JSON.

    :param url: url to a JSON file
    :return: record metadata from file
    """
    res = requests.get(url)
    return res.json()['records']

def crawled_recently(id):
    if not os.path.exists("files/institutions/{}/{}.json".format(id, id)):
        return False
    now = datetime.now()
    if now - timedelta(hours=24) <= datetime.fromtimestamp(
                os.path.getmtime("./files/institutions/{}/{}.json".format(id, id))) <= now:
        return True
    return False

def return_count():
    ingests = glob("files/ingests/*.json")
    recent = sorted(ingests)[-1]
    with open(recent, "r") as inf:
        records = json.load(inf)
    data_providers = {}
    for record in records:
        data_provider = record['dataProvider']
        if "missouri digital heritage" in data_provider.lower():
            data_provider = "Missouri Digital Heritage"
        if data_provider in data_providers:
            data_providers[data_provider] += 1
        else:
            data_providers[data_provider] = 1
    inf.close()
    data_providers = dict(sorted(data_providers.items(), key=lambda item: item[1], reverse=True))
    print("Total: "+str(len(records)))
    for provider, value in data_providers.items():
        print(f"{provider}: {value}")
