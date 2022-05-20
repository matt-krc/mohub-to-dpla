from bs4 import BeautifulSoup
from templates import Template, dpla_template
import utils
from map_list import map_list


class Record:
    def __init__(self, record, decorators):
        self.record: BeautifulSoup.element.Tag = record
        self.header: BeautifulSoup.element.Tag = self.record.find('header')
        self.metadata_prefix: str = decorators['metadata_prefix']
        self.metadata: BeautifulSoup.element.Tag = self.record.find('metadata').find(self.metadata_prefix)
        self.institution: str = decorators['institution']
        self.institution_id: str = decorators['institution_id']
        self.institution_prefix: str = decorators['institution_id_prefix']
        self.exclude: str = decorators['exclude']
        self.oai_url: str = decorators['oai_url']
        self.parsed_record: dict = self.parse()
        self.parsed_metadata: dict = self.parsed_record['metadata']
        self.parsed_header: dict = self.parsed_record['header']
        self.url: str = self.get_urls()
        self.thumbnail: str = self.get_urls("thumbnail")

    def __bool__(self):
        return type(self.parsed_record) == dict

    def parse(self):
        record = self.record
        record['header'] = self.clean_fields(self.header)

        if 'setspec' in record['header']:
            if record['header']['setspec'][0] in self.exclude:
                return False

        record['metadata'] = self.clean_fields(self.metadata)

        return record

    def map(self):
        if type(self.record) == bool:
            return False
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
                metadata["sourceResource"][
                    "rights"] = "NO COPYRIGHT - UNITED STATES\nThe organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information."
                metadata["sourceResource"]["format"] = utils.format_metadata("type", self.parsed_metadata, "string")
                metadata["sourceResource"]["creator"] = utils.format_metadata("contributor", self.parsed_metadata)
                metadata["@id"] = "missouri--urn:data.mohistory.org:" + self.parsed_header["identifier"][0]

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
            url, thumbnail = map_list[institution_id](metadata)
        except KeyError:
            url, thumbnail = False, ""

        return thumbnail if type == "thumbnail" else url

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
            for field, url in self.check_if_url(k, v).items():
                if field not in urls:
                    urls[field] = url
        return urls

