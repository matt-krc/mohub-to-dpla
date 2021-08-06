# mohub-to-dpla

A set of scripts to crawl OAI feeds from MoHub member institutions and convert the data into DPLA-formatted metadata for quarterly ingests.

`python main.py` sets the whole process in motion and creates a line-separated JSON main ingest file as well as reports for each institution. There's several optional arguments that can be set based on what one wants to do.

`dpla.py` contains a class to interact with and get data from the DPLA API. It's not used in the main ingest/crawl process. It should either be updated if necessary, or deleted at some point.

`get_data.py` similarly is not used in the main process, but was used during the initial building process to compare data from previous MoHub ingests.

`maps.py` are the institution-specific metadata mappings between OAI and DPLA-formatted metadata. Ideally, this will be done in a cleaner way, but at this point almost each institution has its own mapping (though some are generalized - all CONTENTdm feeds are grouped together, for example).

`OAI.py` contains classes for OAI feeds and individual OAI records. It also contains some functions for transforming metadata. There's probably some more cleaning to be done in the future here.

`utils.py` contains various helper functions.

`validate.py` contains function that were used in building the process to compare data. It's not really used.

Currently, the data component is a JSON file with an array of objects corresponding to each institution. Fields required are:

```
{
  "institution": // the full name of the institution,
  "id": // unique identifier for the institution,
  "@id_prefix": // an important legacy identifier used by DPLA in certain metadata fields, drawn from previous MoHub ingests. At some point these should be replaced with more intuitive identifiers, but there are issues of persistence that we have to ask DPLA about.
  "url": // URL to the OAI endpoint,
  "metadata_prefix": // Preferred metadata prefix for the institution,
  "include": // An array of specific collection names to include. If present, only collections listed will be crawled.
  "exclude": // An array of specific collection names to exclude. If present, all but these collections will be crawled.
}
```

