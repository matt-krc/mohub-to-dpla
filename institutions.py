import json

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


def get(id="all"):
    if id == "all":
        return data
    else:
        for d in data:
            if d['id'] == id:
                return d
            else:
                return False


def get_all_ids():
    return [d['id'] for d in data]