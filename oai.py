import json
import requests
from bs4 import BeautifulSoup
import sys
import maps
import time
import utils


class OAI:
    def __init__(self, row, verbose=False):
        self.url = row['url']
        self.metadata_prefix = row['metadata_prefix']
        self.institution = row['institution']
        self.institution_id = row['id']
        self.institution_id_prefix = row['@id_prefix']
        self.include = row['include']
        self.exclude = row['exclude']
        self.verbose = verbose

    def oai_request(self,  verb):
        """
        Instatiates an OAI feed request, given an OAI verb.

        :param verb:
        :return:
        """
        params = {
            "verb": verb
        }
        try:
            res = requests.get(self.url, params=params)
        except requests.exceptions.MissingSchema as e:
            return False
        soup = BeautifulSoup(res.content, 'html.parser')

        return soup

    def get_metadata_prefix(self, url):
        """
        Returns an OAI feed's available metadata prefixes.

        :param url: string
        :return: list of metadata prefixes available
        """
        verb = "ListMetadataFormats"
        soup = self.oai_request(url, verb)
        if not soup:
            print("Missing schema error for: {}".format(url))
            return False
        metadata_prefix = [m.getText() for m in soup.find_all('metadataprefix')]

        return metadata_prefix

    def get_institution_name(self):
        """
        For a given OAI feed, return the institution's name as stored in the OAI feed's 'Identify' endpoint.

        :param url: root url for an OAI feed
        :return: string name of the institution
        """

        verb = "Identify"
        soup = self.oai_request(self.url, verb)
        try:
            name = soup.find('repositoryname').getText()
        except AttributeError as e:
            raise
        return name

    def list_sets(self, url):
        """
        For a given OAI feed, request the ListSets endpoint and return a list of sets and set IDs

        :param url: root url for an OAI feed
        :return: list of sets for a given OAI feed
        """
        verb = "ListSets"
        soup = self.oai_request(url, verb)
        sets = [{"setSpec": set["setSpec"], "setName": set["setName"]} for set in soup.find_all("set")]

        return sets

    def crawl(self):
        out = []
        url = self.url
        metadata_prefix = self.metadata_prefix
        resumption_token = True
        records = True
        params = {
            "verb": "ListRecords",
            "metadataPrefix": self.metadata_prefix
        }
        # TODO: If Include is a list it needs to be iterated through
        if self.include:
            params["set"] = self.include

        timeouts = 0
        skipped = 0

        while records and resumption_token:
            sys.stdout.write("\r{} records added".format(len(out)))
            sys.stdout.flush()
            # Some feeds are touchy about requesting too fast, so we pause for 5 seconds if a request error is encountered.
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
            params['resumptionToken'] = resumption_token
            for record in records:
                if not record.find('metadata') or not record.find('header'):
                    return False
                # TODO: figure out if metadata prefix can be inferred from element
                if metadata_prefix == 'oai_dc' or metadata_prefix == 'oai_qdc':
                    metadata_prefix = '{}:dc'.format(metadata_prefix)
                decorators = {
                    "metadata_prefix": metadata_prefix,
                    "institution": self.institution,
                    "institution_id": self.institution_id,
                    "institution_id_prefix": self.institution_id_prefix,
                    "exclude": self.exclude
                }
                out_record = Record(record, decorators)
                if out_record.is_deleted():
                    return False
                out_record = out_record.parse()
                if out_record:
                    out.append(out_record)
                else:
                    skipped += 1
        print(f"\n{skipped} items were skipped.")
        return out


class Record:
    def __init__(self, record, decorators):
        self.oai_record = record
        self.header = record.find('header')
        self.metadata_prefix = decorators['metadata_prefix']
        self.metadata = record.find('metadata').find(self.metadata_prefix)
        self.record = {
                    "header": self.header,
                    "metadata": self.metadata,
                    "institution": decorators['institution'],
                    "institution_id": decorators['institution_id'],
                    "institution_prefix": decorators['institution_id_prefix'],
                    "exclude": decorators['exclude']
                }

    def parse(self):
        record = self.record
        institution_id = record['institution_id']
        dpla_row = self.record_template()
        record['header'] = self.clean_fields(record['header'])

        if 'setspec' in record['header']:
            if record['header']['setspec'][0] in record['exclude']:
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

    def record_template(self):
        """
        Default record template for DPLA records

        :return:
        """
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

    def is_deleted(self):
        header = self.header
        if header.has_attr('status'):
            if header['status'] == 'deleted':
                return True
            else:
                return False
        else:
            return False

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
                row[el.name] = utils.make_list_flat([v for k, v in self.clean_fields(el).items()])

            else:
                row[el.name] = self.clean_fields(el)

        return row
