import json
import requests
from dpla import DPLA
import env
from glob import glob


def get_dpla():
    out = []
    for i in DPLA.get_institutions('Missouri Hub'):
        print(i)
        out.extend(DPLA().crawl_metadata(i, env.DPLA_KEY))

    with open("./files/mohub_dpla.json", "w") as outf:
        json.dump(out, outf, indent=4)

def get_identifiers():
    with open('./files/mohub_dpla.json', 'r') as inf:
        data = json.load(inf)

    ids = {}
    for row in data:
        id = ":".join(row["@id"].split(":")[:-1])
        institution = row['dataProvider']
        if institution not in ids:
            ids[institution] = [id]
        else:
            if id not in ids[institution]:
                ids[institution].append(id)

    with open("./files/institution_identifiers.json", "w") as outf:
        json.dump(ids, outf, indent=4)

def get_prefixes():
    infs = glob('./files/mohub_dpla.json')
    for fn in infs:
        with open(fn, "r") as inf:
            data = json.load(inf)
        prefixes = []
        for row in data:
            if "Linda Hall Library" not in row["dataProvider"]:
                continue
            id = row['@id']
            isShownAt = row["isShownAt"]
            root = "/".join(isShownAt.split("/")[2:3])
            prefix = ":".join(id.split(":")[:-1])
            # print(prefix)
            if prefix not in prefixes:
                print(prefix)
                prefixes.append(prefix)

def transform():
    with open('./files/mohub_dpla.json', 'r') as inf:
        data = json.load(inf)
    mhs = []

    for row in data:
        if row["dataProvider"] == "Missouri Historical Society":
            mhs.append(row)

    with open("./files/institutions/mhs_previous.json", "w") as outf:
        json.dump(mhs, outf, indent=4)

def convert_to_csv():
    return True

# get_prefixes()
transform()

