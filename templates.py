from utils import format_metadata


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
                "title": format_metadata("title", metadata),
                "description": format_metadata("description", metadata),
                "subject": format_metadata("subject", metadata),
                "temporal": [
                    {
                        "displayDate": format_metadata("date", metadata, "string")  # TODO: Parse start/end dates
                    }
                ],
                "identifier": [record.url],
                "creator": format_metadata("creator", metadata),
                "language": format_metadata("language", metadata),
                "rights": format_metadata("rights", metadata, "string"),
                "@id": format_metadata("identifier", header, "string"),
                "format": format_metadata("format", metadata, "string")
            },
            # ID Prefix = {hub_id}--{institution_id}:oai:{oai_url}
            "@id": self.record.institution_prefix + ":" + format_metadata("identifier", header, "string").split(":")[-1]
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
                "title": format_metadata("titleinfo.title", metadata),
                "description": format_metadata("abstract", metadata),
                "subject": format_metadata("subject", metadata),
                "temporal": [{
                    "start": format_metadata("origininfo.dateissued_start", metadata, "string"),
                    "end": format_metadata("origininfo.dateissued_end", metadata, "string"),
                    "displayDate": format_metadata("origininfo.dateissued_start", metadata,
                                                   "string") + "/" + format_metadata("origininfo.dateissued_end",
                                                                                     metadata,
                                                                                     "string") if not format_metadata(
                        "origininfo.dateissued", metadata, "string") else format_metadata("origininfo.dateissued",
                                                                                          metadata, "string")
                }],
                "identifier": record.url,
                "creator": format_metadata("name", metadata),
                "language": format_metadata("language", metadata),
                "rights": format_metadata("accesscondition", metadata, "string"),
                "@id": format_metadata("identifier", header, "string"),
                "format": format_metadata("genre", metadata, "string")
            },
            "@id": record.institution_prefix + ":" + format_metadata("identifier", header, "string").split(":")[-1]
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
