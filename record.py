from bs4 import BeautifulSoup
from map import Map
import utils


class Record:
    def __init__(self, record, decorators):
        self.record: BeautifulSoup.element.Tag = record
        self.header: BeautifulSoup.element.Tag = self.record.find('header')
        self.metadata_prefix: str = decorators['metadata_prefix']
        self.metadata: BeautifulSoup.element.Tag = self.record.find('metadata').find(self.metadata_prefix)
        self.institution: str = decorators['institution']
        self.institution_id: str = decorators['institution_id']
        self.institution_prefix: str = decorators['institution_id_prefix']
        # self.institution_prefix: str = self.generate_id_prefix()
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
        dpla_row = self.record_template()
        metadata_map = Map(self)
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
            metadata = Map(self).default()

            if institution_id == 'kcpl1':
                metadata["sourceResource"]["publisher"] = self.format_metadata("publisher", self.parsed_metadata)
            elif institution_id == 'lhl':
                metadata["sourceResource"][
                    "rights"] = "NO COPYRIGHT - UNITED STATES\nThe organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information."
                metadata["sourceResource"]["format"] = self.format_metadata("type", self.parsed_metadata, "string")
                metadata["sourceResource"]["creator"] = self.format_metadata("contributor", self.parsed_metadata)
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

    def format_metadata(self, field, metadata, return_format="list"):
        value = utils.get_metadata(field, metadata)

        # case 1: field doesn't exist, return either empty list or string
        if not value:
            if return_format == "string":
                return ""
            return []

        # case 2: subjects are always lists of dicts formatted a particular way
        if field == 'subject':
            return [{"name": subj} for subj in value]

        # case 3: language
        if field == 'language':
            return utils.parse_language(value)

        # handle title fields that get split
        if field == "title" or field == "date":
            value = [value[0]]

        # handle all other fields based on current type and format required
        if return_format == "string" and type(value) == list:
            return "; ".join(value)
        elif return_format == "list" and type(value) == str:
            return [value.replace("\n", "")]
        else:
            return value

    def get_urls(self, type="main"):
        institution_id = self.institution_id
        metadata = self.parsed_metadata
        header = self.parsed_header

        if institution_id == 'frb':
            url = self.format_metadata("location.url", metadata, "string")
            thumbnail = self.format_metadata("location.url_preview", metadata, "string")
        elif 'cdm' in header['identifier'][0] or institution_id == 'msu':
            url = metadata["identifier"][-1]
            thumbnail = utils.generate_cdm_thumbnail(metadata["identifier"][-1])
        elif institution_id == 'wustl1':
            url = metadata["identifier"][0] if "identifier" in metadata else ""
            thumbnail = metadata["identifier"][1] if "identifier" in metadata else ""
        elif institution_id == 'wustl2':
            url = [i for i in metadata["identifier"] if "omeka.wustl.edu/omeka/items" in i][0]
            thumbnail = [t for t in metadata["identifier"] if "omeka.wustl.edu/omeka/files/" in t][0] if len([t for t in metadata["identifier"] if "omeka.wustl.edu/omeka/files/" in t]) > 0 else ""
        elif institution_id == "umkc" or institution_id == "umsl":
            url = f"https://dl.mospace.umsystem.edu/{institution_id}/islandora/object/{metadata['identifier'][0]}"
            thumbnail = metadata["identifier.thumbnail"][0] if "identifier.thumbnail" in metadata else ""
        elif institution_id == 'kcpl1':
            url = metadata["relation"][0]
            thumbnail = ""
        elif institution_id == 'kcpl2':
            url = "https://kchistory.org/islandora/object/{}".format(metadata["identifier"][0])
            thumbnail = ""
        elif institution_id == 'lhl':
            url = "https://catalog.lindahall.org/permalink/01LINDAHALL_INST/19lda7s/alma" + \
                 header["identifier"][0].split(":")[-1]
            thumbnail = "https://catalog.lindahall.org/view/delivery/thumbnail/01LINDAHALL_INST/" + \
                        header["identifier"][0].split(":")[-1]
        elif institution_id == 'uni':
            url = metadata["identifier"][0]
            thumbnail = ""
            if 'description' in metadata:
                for d in metadata['description']:
                    if d.split('.')[-1] in ['jpg', 'jpeg'] and d[:4] == 'http':
                        thumbnail = d
                        break
        elif institution_id == 'grinnell':
            if 'identifier' in metadata:
                identifier = metadata['identifier'][0]
                base_url = "https://digital.grinnell.edu"
                collection = identifier.split(':')[0]
                if collection != 'grinnell':
                    url = ""
                    thumbnail = ""
                else:
                    url = f"{base_url}/islandora/object/{identifier}"
                    thumbnail = f"{base_url}/islandora/object/{identifier}/datastream/TN/view"
            else:
                url = ""
                thumbnail = ""
        else:
            url = False
            thumbnail = ""

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
