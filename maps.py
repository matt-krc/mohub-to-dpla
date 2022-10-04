import utils


def cdm(metadata):
    url = metadata["identifier"][-1]
    thumbnail = utils.generate_cdm_thumbnail(metadata["identifier"][-1])
    return url, thumbnail


def frb(metadata):
    url = utils.format_metadata("location.url", metadata, "string")
    thumbnail = utils.format_metadata("location.url_preview", metadata, "string")
    return url, thumbnail


def kcpl1(metadata):
    url = metadata["relation"][0]
    thumbnail = ""
    return url, thumbnail


def kcpl2(metadata):
    url = "https://kchistory.org/islandora/object/{}".format(metadata["identifier"][0])
    thumbnail = ""
    return url, thumbnail


def um(metadata):
    url = f"https://dl.mospace.umsystem.edu/{metadata['institution_id']}/islandora/object/{metadata['identifier'][0]}"
    thumbnail = metadata["identifier.thumbnail"][0] if "identifier.thumbnail" in metadata else ""
    return url, thumbnail


def wustl1(metadata):
    url = metadata["identifier"][0] if "identifier" in metadata else ""
    thumbnail = metadata["identifier"][1] if "identifier" in metadata else ""
    return url, thumbnail


def wustl2(metadata):
    url = [i for i in metadata["identifier"] if "omeka.wustl.edu/omeka/items" in i][0]
    thumbnail = [t for t in metadata["identifier"] if "omeka.wustl.edu/omeka/files/" in t][0] if len(
        [t for t in metadata["identifier"] if "omeka.wustl.edu/omeka/files/" in t]) > 0 else ""
    return url, thumbnail


def lhl(metadata):
    url = "https://catalog.lindahall.org/permalink/01LINDAHALL_INST/19lda7s/alma" + \
          metadata['header']["identifier"][0].split(":")[-1]
    thumbnail = "https://catalog.lindahall.org/view/delivery/thumbnail/01LINDAHALL_INST/" + \
                metadata['header']["identifier"][0].split(":")[-1]
    return url, thumbnail


def grinnell(metadata):
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
    return url, thumbnail


def uni(metadata):
    url = metadata["identifier"][0]
    thumbnail = ""
    if 'description' in metadata:
        for d in metadata['description']:
            if d.split('.')[-1] in ['jpg', 'jpeg'] and d[:4] == 'http':
                thumbnail = d
                break
    return url, thumbnail


def isu(metadata):
    verbose_url = ""
    short_url = ""
    for identifier in metadata['identifier']:
        if identifier.split(':')[0] == 'isu':
            verbose_url = f"https://digitalcollections.lib.iastate.edu/islandora/object/{identifier}"
        elif 'https://n2t.net' in identifier:
            short_url = identifier
    url = verbose_url if verbose_url else short_url

    thumbnail = metadata["identifier.thumbnail"]

    return url, thumbnail
