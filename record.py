from bs4 import BeautifulSoup
from templates import Template, dpla_template
import utils
from map_list import map_list
import sys


class Record:
    def __init__(self, record, decorators):
        self.institution: str = decorators['institution']
        self.institution_id: str = decorators['institution_id']
        self.institution_prefix: str = decorators['institution_id_prefix']
        self.exclude: str = decorators['exclude']
        self.oai_url: str = decorators['oai_url']
        self.metadata_prefix: str = decorators['metadata_prefix']

        self.record: BeautifulSoup.element.Tag = record
        self.parsed_record = {}
        self.header: BeautifulSoup.element.Tag = self.set_header()
        self.parsed_header: dict = self.set_parsed_header()
        self.metadata: BeautifulSoup.element.Tag = self.set_metadata()
        self.parsed_metadata: dict = self.set_parsed_metadata()

        self.url: str = self.get_urls()
        self.thumbnail: str = self.get_urls("thumbnail")
        self.iiif_manifest: str = self.get_urls("iiif")

    def __bool__(self):
        return type(self.parsed_record) == dict

    def set_header(self):
        if not self.record.find('header'):
            raise OAIRecordException('No header in row')
        return self.record.find('header')

    def set_metadata(self):
        if not self.record.find('metadata'):
            if self.is_deleted():
                raise OAIRecordException('Record is deleted')
            raise OAIRecordException('No metadata in row', self.parsed_header)
        return self.record.find('metadata').find(self.metadata_prefix)

    def set_parsed_header(self):
        self.parsed_record['header'] = self.clean_fields(self.header)
        if 'setspec' in self.parsed_record['header']:
            if self.parsed_record['header']['setspec'][0] in self.exclude:
                raise OAIRecordException('Collection excluded from crawl', self.parsed_record['header'])
        return self.parsed_record['header']

    def set_parsed_metadata(self):
        self.parsed_record['metadata'] = self.clean_fields(self.metadata)
        return self.parsed_record['metadata']

    def map(self):
        institution_id = self.institution_id
        dpla_row = dpla_template()
        metadata_map = Template(self)
        if institution_id == 'frb':
            metadata = metadata_map.frb()
        else:
            if institution_id == "mdh" and "publisher" in self.parsed_metadata:
                # Rules for Missouri Digital Heritage
                self.institution = self.parsed_metadata["publisher"][0] + " through Missouri Digital Heritage"
            elif institution_id == "shsm":
                # Rules for State Historical Society
                collection = self.parsed_header["identifier"][0].split(":")[-1].split("/")[0]
                self.institution_prefix = self.institution_prefix.replace("<collection>", collection)
            elif institution_id == "slu":
                # Rules for Saint Louis University
                collection = self.parsed_header["identifier"][0].split(":")[-1].split("/")[0]
                if collection == "ong":
                    self.parsed_metadata["description"] = []
            elif institution_id == "sgcl":
                collection = self.parsed_header["identifier"][0].split(":")[-1].split("/")[0]
                if collection == "p16792coll1":
                    self.parsed_metadata["rights"] = "The Ozarks Genealogical Society, Inc. offers access to this collection for " \
                                         "educational and personal research purposes only.  Materials within the collection " \
                                         "may be protected by the U.S. Copyright Law (Title 17, U.S.C.).  It is the " \
                                         "researcher's obligation to determine and satisfy copyright or other use restriction " \
                                         "when publishing or otherwise distributing materials within the collection."
            elif institution_id == 'grinnell':
                for k, v in dict(self.parsed_metadata).items():
                    if len(k.split("_")) > 1 and k.split("_")[1][:4] == 'http':
                        self.parsed_metadata[k.split("_")[0]] = v
                        del self.parsed_metadata[k]
            metadata = Template(self).default()

            if institution_id == 'kcpl1':
                metadata["sourceResource"]["publisher"] = utils.format_metadata("publisher", self.parsed_metadata)
            elif institution_id == 'lhl':
                metadata["sourceResource"]["rights"] = "NO COPYRIGHT - UNITED STATES\nThe organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information."
                metadata["rights"] = "http://rightsstatements.org/vocab/NoC-US/1.0/"
                metadata["rightsCategory"] = "NO COPYRIGHT - UNITED STATES"
                metadata["sourceResource"]["format"] = utils.format_metadata("type", self.parsed_metadata, "string")
                metadata["sourceResource"]["creator"] = utils.format_metadata("contributor", self.parsed_metadata)
                metadata["@id"] = "missouri--urn:data.mohistory.org:" + self.parsed_header["identifier"][0]
            elif institution_id == 'isu':
                metadata["sourceResource"]["rights"] = "CC0"
                metadata["rights"] = "http://creativecommons.org/publicdomain/zero/1.0/"
                metadata["sourceResource"]["contributor"] = "Iowa State University. Special Collections and Archives"
                if "coverage" in self.parsed_metadata.keys():
                    metadata["sourceResource"]["spatial"] = self.parsed_metadata["coverage"][0]
                    del metadata["sourceRecord"]["coverage"]
                if "format" in self.parsed_metadata.keys():
                    metadata["sourceResource"]["extent"] = self.parsed_metadata["format"][0]
                    del metadata["sourceResource"]["format"]

        if not metadata:
            return False

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

        # Conditional fields, not necessarily in every record
        if "rights" in metadata:
            dpla_row["rights"] = metadata["rights"]

        if "rightsCategory" in metadata:
            dpla_row["rightsCategory"] = metadata["rightsCategory"]

        if "iiifManifest" in metadata:
            dpla_row["iiifManifest"] = metadata["iiifManifest"]

        return dpla_row

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

    def get_urls(self, type="main"):
        institution_id = self.institution_id
        metadata = self.parsed_metadata
        header = self.parsed_header
        metadata['institution_id'] = institution_id
        metadata['header'] = header

        try:
            url, thumbnail, iiif_manifest = map_list[institution_id](metadata)
        except KeyError:
            url, thumbnail, iiif_manifest = False, "", ""

        if institution_id not in map_list:
            # TODO kick off looking for URLs
            raise OAIRecordException('No mapping found for institution', self.record)

        if type == 'main' and not url:
            raise OAIRecordException('No URL could be produced for this record', self.record)

        if type == 'thumbnail':
            return thumbnail
        elif type == 'iiif':
            return iiif_manifest
        else:
            return url


    def check_if_url(self, key, value):
        urls = {}
        if type(value) == list:
            for index, v in enumerate(value):
                url_match = self.check_if_url(index, v)
                if url_match:
                    urls[f"{key}[{index}]"] = v
        elif type(value) == dict:
            for k, v in value.items():
                url_match = self.check_if_url(k, v)
                if url_match:
                    for field, url in url_match.items():
                        urls[f"{key}.{field}"] = url
        else:
            if value[:4] == 'http':
                urls[key] = value
            else:
                return False
        return urls

    def search_for_urls(self):
        urls = {}
        metadata = self.parsed_record['metadata']
        for k, v in metadata.items():
            potential_urls = self.check_if_url(k, v)
            if potential_urls:
                for field, url in potential_urls.items():
                    if field not in urls:
                        urls[field] = url
        return urls


class OAIRecordException(Exception):
    def __init__(self, message, record={}):
        self.message = message
        self.record = record
