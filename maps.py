# DEPRECATED: maps have been moved to record.py and map.py

import utils

def format_metadata(field, metadata, format="list"):
    value = utils.get_metadata(field, metadata)

    # case 1: field doesn't exist, return either empty list or string
    if not value:
        if format == "string":
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
    if format == "string" and type(value) == list:
        return "; ".join(value)
    elif format == "list" and type(value) == str:
        return [value.replace("\n", "")]
    else:
        return value


def default_row_oai_dc(oai_row, url, thumbnail=""):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    metadata = {
        "url": url,
        "institution": oai_row["institution"],
        "thumbnail": thumbnail,
        "sourceResource": {
            "title": format_metadata("title", metadata),
            "description": format_metadata("description", metadata),
            "subject": format_metadata("subject", metadata),
            "temporal": [
                {
                    "displayDate": format_metadata("date", metadata, "string") # TODO: Parse start/end dates
                }
            ],
            "identifier": [url],
            "creator": format_metadata("creator", metadata),
            "language": format_metadata("language", metadata),
            "rights": format_metadata("rights", metadata, "string"),
            "@id": format_metadata("identifier", header, "string"),
            "format": format_metadata("format", metadata, "string")
        },
        # ID Prefix = {hub_id}--{institution_id}:oai:{oai_url}
        "@id": oai_row["institution_prefix"] + ":" + format_metadata("identifier", header, "string").split(":")[-1]
    }

    return metadata


def cdm(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    if oai_row["institution_id"] == "mdh" and "publisher" in metadata:
        # Exceptions for Missouri Digital Heritage
        oai_row["institution"] = metadata["publisher"][0] + " through Missouri Digital Heritage"
    elif oai_row["institution_id"] == "shsm":
        # Exceptions for State Historical Society
        collection = header["identifier"][0].split(":")[-1].split("/")[0]
        oai_row["institution_prefix"] = oai_row["institution_prefix"].replace("<collection>", collection)
    elif oai_row["institution_id"] == "slu":
        # Exceptions for Saint Louis University
        collection = header["identifier"][0].split(":")[-1].split("/")[0]
        if collection == "ong":
            metadata["description"] = []
    elif oai_row["institution_id"] == "sgcl":
        collection = header["identifier"][0].split(":")[-1].split("/")[0]
        if collection == "p16792coll1":
            metadata["rights"] = "The Ozarks Genealogical Society, Inc. offers access to this collection for " \
                                 "educational and personal research purposes only.  Materials within the collection " \
                                 "may be protected by the U.S. Copyright Law (Title 17, U.S.C.).  It is the " \
                                 "researcher's obligation to determine and satisfy copyright or other use restriction " \
                                 "when publishing or otherwise distributing materials within the collection."

    url = metadata["identifier"][-1]
    thumbnail = utils.generate_cdm_thumbnail(metadata["identifier"][-1])

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    return metadata


def wustl(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = metadata["identifier"][0] if "identifier" in metadata else ""
    try:
        thumbnail = metadata["identifier"][1]
    except (KeyError, IndexError) as e:
        thumbnail = ""

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    return metadata


def um(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = "https://dl.mospace.umsystem.edu/{}/islandora/object/{}"\
        .format(oai_row["institution_id"], metadata["identifier"][0])
    thumbnail = metadata["identifier.thumbnail"][0] if "identifier.thumbnail" in metadata else ""

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    return metadata


def kcpl1(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = metadata["relation"][0]
    publisher = format_metadata("publisher", metadata)

    metadata = default_row_oai_dc(oai_row, url)
    metadata["sourceResource"]["publisher"] = publisher

    return metadata


def kcpl2(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = "https://kchistory.org/islandora/object/{}".format(metadata["identifier"][0])

    metadata = default_row_oai_dc(oai_row, url)

    return metadata


def omeka_wustl(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = [i for i in metadata["identifier"] if "omeka.wustl.edu/omeka/items" in i][0]
    try:
        thumbnail = [t for t in metadata["identifier"] if "omeka.wustl.edu/omeka/files/" in t][0]
    except IndexError as e:
        thumbnail = ""

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    return metadata


def lhl(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]
    _metadata = metadata

    url = "https://catalog.lindahall.org/permalink/01LINDAHALL_INST/19lda7s/alma" + header["identifier"][0].split(":")[-1]
    thumbnail = "https://catalog.lindahall.org/view/delivery/thumbnail/01LINDAHALL_INST/" + header["identifier"][0].split(":")[-1]

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    metadata["sourceResource"]["rights"] = "NO COPYRIGHT - UNITED STATES\nThe organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information."
    metadata["sourceResource"]["format"] = format_metadata("type", _metadata, "string")
    metadata["sourceResource"]["creator"] = format_metadata("contributor", _metadata)
    metadata["@id"] = "missouri--urn:data.mohistory.org:" + header["identifier"][0]

    return metadata


def fraser(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = format_metadata("location.url", metadata, "string")
    thumbnail = format_metadata("location.url_preview", metadata, "string")

    metadata = {
        "url": url,
        "institution": oai_row["institution"],
        "thumbnail": thumbnail,
        "sourceResource": {
            "title": format_metadata("titleinfo.title", metadata),
            "description": format_metadata("abstract", metadata),
            "subject": format_metadata("subject", metadata),
            "temporal": [{
                "start": format_metadata("origininfo.dateissued_start", metadata, "string"),
                "end": format_metadata("origininfo.dateissued_end", metadata, "string"),
                "displayDate": format_metadata("origininfo.dateissued_start", metadata, "string") + "/" + format_metadata("origininfo.dateissued_end", metadata, "string") if not format_metadata("origininfo.dateissued", metadata, "string") else format_metadata("origininfo.dateissued", metadata, "string")
            }],
            "identifier": url,
            "creator": format_metadata("name", metadata),
            "language": format_metadata("language", metadata),
            "rights": format_metadata("accesscondition", metadata, "string"),
            "@id": format_metadata("identifier", header, "string"),
            "format": format_metadata("genre", metadata, "string")
        },
        "@id": oai_row["institution_prefix"] + ":" + format_metadata("identifier", header, "string").split(":")[-1]
    }

    return metadata


# Iowa maps
def grinnell(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    # This feed has some field values with XML schema tags that need to be removed
    # TODO: Move this somewhere else?
    for k, v in dict(metadata).items():
        if len(k.split("_")) > 1 and k.split("_")[1][:4] == 'http':
            metadata[k.split("_")[0]] = v
            del metadata[k]

    try:
        identifier = metadata['identifier'][0]
    except KeyError as e:
        return False

    base_url = "https://digital.grinnell.edu"

    collection = identifier.split(':')[0]
    if collection != 'grinnell':
        return False

    url = f"{base_url}/islandora/object/{identifier}"
    thumbnail = f"{base_url}/islandora/object/{identifier}/datastream/TN/view"

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    return metadata


def uni(oai_row):
    metadata = oai_row["metadata"]
    header = oai_row["header"]

    url = metadata["identifier"][0]
    thumbnail = ""
    if 'description' in metadata:
        for d in metadata['description']:
            if d.split('.')[-1] in ['jpg', 'jpeg'] and d[:4] == 'http':
                thumbnail = d
                break

    metadata = default_row_oai_dc(oai_row, url, thumbnail)

    return metadata

