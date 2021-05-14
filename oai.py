import json
import requests
from bs4 import BeautifulSoup
import sys
import maps
import time
from iso639 import languages
from dateutil import parser
from urllib.parse import urlparse
import pandas as pd


def parse_date(datestr):
    try:
        parsed = parser.parse(datestr)
    except ValueError as e:
        print(f"{datestr} could not be parsed as a date. Skipping.")
        return False

    return parsed.strftime("%B %-d, %Y")


def get_metadata(field, metadata):
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


def make_list_flat(l):
    flist = []
    flist.extend([l]) if (type(l) is not list) else [flist.extend(make_list_flat(e)) for e in l]
    return flist

def split_values(row):
    '''
    Splits values that have semicolons for certain fields.
    '''
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


class OAI:

    def __init__(self, verbose=False):
        self.verbose = False

    def generate_cdm_thumbnail(self, url):
        try:
            collection = url.split("/")[url.split("/").index("collection") + 1]
        except ValueError as e:
            print(url)
            raise
        record_id = url.split("/")[-1]
        o = urlparse(url)
        base = "{}://{}".format(o.scheme, o.netloc)
        thumbnail = "{}/utils/getthumbnail/collection/{}/id/{}".format(base, collection, record_id)

        return thumbnail


    def parse_language(self, language_list):
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
                    lng = eval("languages.get({}='{}')".format(code, language))
                    outlist.append({f"iso639_3": lng.part3, "name": lng.name})
                    language_found = True
                    break
                except (KeyError, SyntaxError) as e:
                    continue
            if not language_found:
                if self.verbose:
                    print("WARNING: Language {} could not be converted to an ISO 639.3 code".format(language))

        return outlist

    def clean_fields(self, element):
        """
        Recursive function that parses out nested metadata fields

        :param element:
        :return:
        """

        row = {}
        if not element.findChildren(recursive=False):
            return [element.getText().strip()]
        for el in element.findChildren(recursive=False):
            # split DC fields to just get the name without the namespace
            if len(el.name.split(':')) > 1:
                el.name = el.name.split(':')[1]

            if 'relateditem' in el.name.lower() or 'tableofcontents' in el.name.lower():
                continue

            if el.attrs:
                el.name = el.name + "_" + list(el.attrs.values())[0]

            # Handle FRASER's weird subject hierarchy
            if element.name == el.name:
                return [el.getText().strip()]

            if element.name == 'name':
                # TODO concatenate namePartDate if it exists
                return [element.find('namepart').getText()]

            if el.name == "coverage":
                # Coverage gets "counted" as subject
                el.name = "subject"

            # TODO: Publisher is used as author in UMKC, figure out if it conflicts (it does -- MDH uses it for attribution)
            # if el.name == 'publisher':
            #    el.name = "creator"

            # Handle same field names
            if el.name in row:
                if "titleinfo_" in el.name:
                    row[el.name]["title"].extend(self.clean_fields(el)["title"])
                else:
                    row[el.name].extend(self.clean_fields(el))
            elif el.name == 'subject' and type(self.clean_fields(el)) == dict:
                row[el.name] = make_list_flat([v for k, v in self.clean_fields(el).items()])

            else:
                row[el.name] = self.clean_fields(el)

        return row

    def row_template(self):
        '''
        Return default values for a record
        
        '''
        row = {
            "@context": "http://dp.la/api/items/context",
            "isShownAt": None,  # URL to object
            "dataProvider": "",
            "@type": "ore:Aggregation",
            "hasView": {
                "@id": None  # URL to object
            },
            "provider": {
                "@id": "http://dp.la/api/contributor/missouri-hub",
                "name": "Missouri Hub"
            },
            "object": None,  # thumbnail
            "aggregatedCHO": "#sourceResource",
            "sourceResource": {
                "title": [],
                "description": [],
                "subject": [],
                "temporal": [],
                "rights": "",
                "@id": "",  # OAI ID
                "language": [
                    {
                        "iso639_3": "eng",
                        "name": "English"
                    }
                ],
                "stateLocatedIn": [
                    {
                        "name": "Missouri"
                    }
                ],
                "format": "",
                "identifier": [],
                "creator": [],
                "specType": []
            },
            "@id": ""
        }

        return row

    def is_deleted(self, header):
        if header.has_attr('status'):
            if header['status'] == 'deleted':
                return True
            else:
                return False
        else:
            return False

    def output_json(self, metadata):
        with open('test.json', 'w') as outf:
            json.dump(metadata, outf, indent=4)
        print("Wrote example metadata to test.json")
        sys.exit()

    def map_to_dpla(self, metadata, dpla_row):
        """
        Maps institution-specific formatted metadata to a DPLA-formatted record

        :param metadata:
        :param dpla_row:
        :return:
        """
        dpla_row["isShownAt"] = metadata["url"]
        dpla_row["hasView"]["@id"] = metadata["url"]
        dpla_row["dataProvider"] = metadata["institution"]
        dpla_row["@id"] = metadata["@id"]
        dpla_row["object"] = metadata["thumbnail"]
        dpla_row["sourceResource"] = metadata["sourceResource"]

        return dpla_row

    def get_institution_prefix(self, institution):
        with open("./files/institution_identifiers.json", "r") as inf:
            data = json.load(inf)

        if institution in data:
            if len(data[institution]) == 1:
                return data[institution][0]
            elif len(data[institution]) > 1:
                raise Exception(f"Institution has more than one ID: {institution}")
        else:
            raise Exception(f"Institution doesn't have prefix: {institution}")


    def parse(self, record, institution, metadata_prefix, institution_id, exclude):
        dpla_row = self.row_template()

        if not record.find('metadata') or not record.find('header'):
            if self.verbose:
                print("Either a header or metadata could not be found.")
            return False

        header = record.find('header')
        if self.is_deleted(header):
            return False

        if metadata_prefix == 'oai_dc' or metadata_prefix == 'oai_qdc':
            metadata_prefix = '{}:dc'.format(metadata_prefix)
        metadata = record.find('metadata').find(metadata_prefix)

        record = {
            "header": header,
            "metadata": metadata,
            "institution": institution,
            "institution_id": institution_id,
            "institution_prefix": self.get_institution_prefix(institution)
        }

        record['header'] = self.clean_fields(record['header'])

        if 'setspec' in record['header']:
            if record['header']['setspec'][0] in exclude:
                return False

        record['metadata'] = self.clean_fields(record['metadata'])

        if 'cdm' in record['header']['identifier'][0] or institution_id == 'msu':
            metadata = maps.cdm(record)
        elif 'omeka' in record['header']['identifier'][0]:
            metadata = maps.omeka_wustl(record)
        elif institution_id == "umkc" or institution_id == "umsl":
            metadata = maps.um(record)
        elif "wustl" in institution_id:
            metadata = maps.wustl(record)
        elif 'fraser' in record['header']['identifier'][0]:
            metadata = maps.fraser(record)
        elif 'kcpl1' in institution_id:
            metadata = maps.kcpl1(record)
        elif 'kcpl2' in institution_id:
            metadata = maps.kcpl2(record)
        elif 'lhl' in institution_id:
            metadata = maps.lhl(record)
        else:
            raise Exception("No metadata mapping found")

        out_row = self.map_to_dpla(metadata, dpla_row)

        return out_row

    def get_urls(self, inf):
        with open(inf, "r") as inf:
            data = json.load(inf)
        return [row['url'] for row in data if row['url']]

    def oai_request(self, url, verb):
        params = {
            "verb": verb
        }
        try:
            res = requests.get(url, params=params)
        except requests.exceptions.MissingSchema as e:
            return False
        soup = BeautifulSoup(res.content, 'html.parser')

        return soup

    def get_metadata_prefix(self, url):
        verb = "ListMetadataFormats"
        soup = self.oai_request(url, verb)
        if not soup:
            print("Missing schema error for: {}".format(url))
            return False
        metadata_prefix = [m.getText() for m in soup.find_all('metadataprefix')]

        # oai_dc or MODS is preferred schema
        if 'oai_dc' in metadata_prefix:
            metadata_prefix = 'oai_dc'
        elif 'mods' in metadata_prefix:
            metadata_prefix = 'mods'
        else:
            print("Cannot handle metadata for {}".format(url))
            return False

        return metadata_prefix

    def get_institution(self, url):
        verb = "Identify"
        soup = self.oai_request(url, verb)
        try:
            name = soup.find('repositoryname').getText()
        except AttributeError as e:
            name = "Not found"
        return name

    def get_institution_sets(self, url):
        verb = "ListSets"
        soup = self.oai_request(url, verb)
        sets = [{"setSpec": set["setSpec"], "setName": set["setName"]} for set in soup.find_all("set")]

        return sets

    def write_csv(self, data, outpath):
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



    def harvest(self, url, metadata_prefix, institution, institution_id, exclude, include=None):
        """
        Loops through OAI feed and returns DPLA-formatted metadata

        :param url: endpoint for the OAI feed
        :param metadata_prefix: metadata prefix used in OAI feed
        :return:
        """

        resumption_token = True
        records = True
        params = {
            "verb": "ListRecords",
            "metadataPrefix": metadata_prefix
        }
        if include:
            params["set"] = include
        out = []
        timeouts = 0
        skipped = 0

        while records and resumption_token:
            # print(f"{len(out)} records added.")
            sys.stdout.write("\r{} records added".format(len(out)))
            sys.stdout.flush()
            try:
                res = requests.get(url, params=params)
            except requests.exceptions.ConnectionError as e:
                timeouts += 1
                print("\nRequest timed out. Waiting 5 seconds and trying again. Attempt {}".format(timeouts))
                if timeouts == 5:
                    print("\nRequest has timed out 5 times. Stopping harvest.")
                    break
                continue
            if res.status_code // 100 == 5:
                print("\nServer error. Waiting 5 seconds and trying request again.")
                print(res.status_code)
                time.sleep(5)
                continue

            # OAI doesn't like it if you're using a resumption token with a metadataPrefix or set param, so we delete it after initial request
            if 'metadataPrefix' in params:
                del params['metadataPrefix']
            if 'set' in params:
                del params['set']

            soup = BeautifulSoup(res.content, 'html.parser')

            records = soup.find_all('record')
            try:
                resumption_token = soup.find('resumptiontoken').getText()
            except AttributeError as e:
                resumption_token = None
            # print(resumption_token)
            params['resumptionToken'] = resumption_token
            for record in records:
                out_row = self.parse(record, institution, metadata_prefix, institution_id, exclude)
                if out_row:
                    out.append(out_row)
                else:
                    skipped += 1
        print(f"\n{skipped} items were skipped.")
        return out
