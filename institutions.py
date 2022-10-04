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

# these are used to construct legacy identifiers
# ids not included just use current id
id_snippets = {
    'frb': 'frbstl_fraser',
    'msu': 'msu_all',
    'kcpl1': 'kcpl_pdr',
    'kcpl2': 'kcpl_pdr',
    'umkc': 'umkc_dl',
    'stlpl': 'slpl_dl',
    'shsm': '<collection>',
    'mdh': 'mdh_all',
    'slu': 'slu_dl',
    'umsl': 'umkc_dl',
    'wustl1': 'wustl_omeka',
    'wustl2': 'wustl_omeka'
}

url_snippets = {
    'stlpl': 'collections.slpl.org',
    'slu': 'cdm.slu.edu',
    'wustl1': 'omeka.wustl.edu',
    'kcpl2': 'pendergastkc.org'
}


def get(institution_id="all"):
    if institution_id == "all":
        return [Institution(d) for d in data]
    else:
        for d in data:
            if d['id'] == institution_id:
                return Institution(d)
        return False


def get_all_ids():
    return [d['id'] for d in data]


class Institution:
    def __init__(self, institution_data):
        # from oai import OAI
        # url and id are required fields, the rest are optional when initializing
        self.url: str = institution_data['url']
        self.id: str = institution_data['id']
        self.name: str = institution_data['institution'] if 'institution' in institution_data else ""
        self.hub = institution_data['hub'] if 'hub' in institution_data else "mohub"
        self.include: list = institution_data['include'] if 'include' in institution_data else []
        self.exclude: list = institution_data['exclude'] if 'exclude' in institution_data else []
        self.id_prefix: str = self.generate_id_prefix()
        self.preferred_metadata_prefix: str = institution_data['metadata_prefix'] if 'metadata_prefix' in institution_data else None
        # self.oai = OAI(self)
        # self.metadata_prefixes = self.oai.get_metadata_prefixes()

    def generate_id_prefix(self):
        prefix_components = []
        institution_id = self.id

        if self.hub == 'mohub':
            prefix_components.append("missouri--urn")
            prefix_components.append("data.mohistory.org")
        elif self.hub == 'iowa':
            prefix_components.append("iowa--urn")

        id_snippet = id_snippets[institution_id] if institution_id in id_snippets else institution_id

        prefix_components.append(id_snippet)

        prefix_components.append("oai")

        if institution_id == 'stlpl':
            url_snippet = 'collections.slpl.org'
        elif institution_id == 'slu':
            url_snippet = 'cdm.slu.edu'
        elif institution_id == 'wustl1':
            url_snippet = 'omeka.wustl.edu'
        elif institution_id == 'kcpl2':
            url_snippet = 'pendergastkc.org'
        elif institution_id == 'umkc' or institution_id == 'umsl':
            url_snippet = "/".join([self.url.split("/")[2], self.url.split("/")[3]]) + "/"
        else:
            url_snippet = self.url.split("/")[2]
        prefix_components.append(url_snippet)

        if institution_id == 'frb':
            prefix_components.append('title')

        return ":".join(prefix_components)

