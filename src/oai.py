import json
import requests
from bs4 import BeautifulSoup
from record import Record
import sys
import time
import sys


class OAI:
    def __init__(self, row, verbose=False):
        self.verbose = verbose
        # OAI initiator can either pass a preformed data object or a URL to an OAI endpoint
        if type(row) == dict:
            self.url = row['url']
            self.metadata_prefix = row['metadata_prefix'] if 'metadata_prefix' in row else self.get_metadata_prefix()
            self.institution = row['institution']
            self.institution_id = row['id']
            self.institution_id_prefix = row['@id_prefix']
            self.include = row['include'] if 'include' in row else []
            self.exclude = row['exclude'] if 'exclude' in row else []
        # TODO: validate URL
        else:
            url = row
            self.url = url
            # self.metadata_prefix = self.get_metadata_prefix()
            # self.institution = self.get_institution_name()


    def oai_request(self, verb):
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

    def get_metadata_prefix(self):
        """
        Returns an OAI feed's available metadata prefixes.

        :param url: string
        :return: list of metadata prefixes available
        """
        verb = "ListMetadataFormats"
        soup = self.oai_request(verb)
        if not soup:
            raise Exception("Missing schema error for: {}".format(self.url))

        metadata_prefixes = [m.getText() for m in soup.find_all('metadataprefix')]

        # Right now we're preferring oai_dc metadata
        if 'oai_dc' in metadata_prefixes:
            metadata_prefix = 'oai_dc'
        # Otherwise we just pick one and hope that it works
        # TODO: Test a bunch of different prefixes to make sure they're interoperable
        else:
            metadata_prefix = metadata_prefixes[0]

        return metadata_prefix

    def identify(self):
        verb = "Identify"
        soup = self.oai_request(verb)

        metadata = soup.find('identify').findAll()

        return {row.name: row.getText() for row in metadata}

    def get_institution_name(self):
        """
        For a given OAI feed, return the institution's name as stored in the OAI feed's 'Identify' endpoint.

        :param url: root url for an OAI feed
        :return: string name of the institution
        """

        verb = "Identify"
        soup = self.oai_request(verb)
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
        potential_urls = {}
        no_map = False

        while records and resumption_token:
            sys.stdout.write("\r{} records added : {} records skipped".format(len(out), skipped))
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
                    skipped += 1
                    continue
                if metadata_prefix == 'oai_dc' or metadata_prefix == 'oai_qdc':
                    metadata_prefix = '{}:dc'.format(metadata_prefix)
                decorators = {
                    "metadata_prefix": metadata_prefix,
                    "institution": self.institution,
                    "institution_id": self.institution_id,
                    "institution_id_prefix": self.institution_id_prefix,
                    "exclude": self.exclude
                }
                try:
                    out_record = Record(record, decorators)
                except TypeError as e:
                    skipped += 1
                    continue
                if out_record.is_deleted():
                    skipped += 1
                    continue
                if out_record.url is False or not out_record.url:
                    if out_record.url is False:
                        if not potential_urls:
                            if not no_map:
                                print("\n\nNo URL mapping could be established for institution.")
                                print("Attempting to find metadata fields containing URLs.")
                                potential_urls = out_record.search_for_urls()
                                no_map = True
                        else:
                            for k, v in out_record.search_for_urls().items():
                                if k not in potential_urls:
                                    potential_urls[k] = v
                    skipped += 1
                    continue
                out_record = out_record.map()
                if out_record:
                    out.append(out_record)
                else:
                    skipped += 1
        if potential_urls:
            print("\n\nFound the following potential URL fields:")
            for field, url in potential_urls.items():
                print(f"{field}: {url}")
            sys.exit()
        print(f"\n{skipped} items were skipped.")
        return out, skipped
