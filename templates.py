class Template:
    def __init__(self, record):
        self.record = record
        self.metadata = record.parsed_metadata
        self.header = record.parsed_header
        self.url = record.url
        self.thumbnail = record.thumbnail

    def default(self):
        record = self.record
        metadata = self.metadata
        header = self.header

        metadata = {
            "url": self.url,
            "institution": self.record.institution,
            "thumbnail": self.thumbnail,
            "sourceResource": {
                "title": record.format_metadata("title", metadata),
                "description": record.format_metadata("description", metadata),
                "subject": record.format_metadata("subject", metadata),
                "temporal": [
                    {
                        "displayDate": record.format_metadata("date", metadata, "string")  # TODO: Parse start/end dates
                    }
                ],
                "identifier": [record.url],
                "creator": record.format_metadata("creator", metadata),
                "language": record.format_metadata("language", metadata),
                "rights": record.format_metadata("rights", metadata, "string"),
                "@id": record.format_metadata("identifier", header, "string"),
                "format": record.format_metadata("format", metadata, "string")
            },
            # ID Prefix = {hub_id}--{institution_id}:oai:{oai_url}
            "@id": self.record.institution_prefix + ":" + record.format_metadata("identifier", header, "string").split(":")[-1]
        }

        return metadata

    def frb(self):
        record = self.record
        metadata = self.metadata
        header = self.header

        metadata = {
            "url": self.url,
            "institution": record.institution,
            "thumbnail": record.thumbnail,
            "sourceResource": {
                "title": record.format_metadata("titleinfo.title", metadata),
                "description": record.format_metadata("abstract", metadata),
                "subject": record.format_metadata("subject", metadata),
                "temporal": [{
                    "start": record.format_metadata("origininfo.dateissued_start", metadata, "string"),
                    "end": record.format_metadata("origininfo.dateissued_end", metadata, "string"),
                    "displayDate": record.format_metadata("origininfo.dateissued_start", metadata,
                                                   "string") + "/" + record.format_metadata("origininfo.dateissued_end",
                                                                                     metadata,
                                                                                     "string") if not record.format_metadata(
                        "origininfo.dateissued", metadata, "string") else record.format_metadata("origininfo.dateissued",
                                                                                          metadata, "string")
                }],
                "identifier": record.url,
                "creator": record.format_metadata("name", metadata),
                "language": record.format_metadata("language", metadata),
                "rights": record.format_metadata("accesscondition", metadata, "string"),
                "@id": record.format_metadata("identifier", header, "string"),
                "format": record.format_metadata("genre", metadata, "string")
            },
            "@id": record.institution_prefix + ":" + record.format_metadata("identifier", header, "string").split(":")[-1]
        }

        return metadata


def dpla_template():
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
