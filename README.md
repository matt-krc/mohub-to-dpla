# heartland-hub-to-dpla

A set of scripts and classes to crawl OAI feeds and convert the data into DPLA-formatted metadata.

Right now the process is oriented towards Heartland Hub and its member institutions but future work may focus on generalizing it to be more institution-agnostic.

# Main Object Classes

- `institutions.py` includes the **Institution** class which contains basic information needed to create an OAI feed object. The main component of an **Institution** are `url` and a unique `id` that feeds into the eventual DPLA metadata.

	```
	from institutions import Institution
	inst = Institution({
		"url": "https://my.org/oai/feed",
		"id": "myid"
	})
	```
	
- `oai.py` includes the **OAI** class object which is instatiated from an ***Institution*** object. From this you can get information from some of the OAI-allowed `verb` parameters as well as crawl the feed to transform it into DPLA-formatted metadata.

	```
	from oai import OAI
	feed = OAI(inst)
	
	print(feed.identify())
	```
	
- `record.py` includes the **Record** class object which is called from the **OAI** `crawl` method. For each individual record in an OAI feed, a **Record** object is instatiated to parse and transform the metadata to a DPLA-formatted metadata object.

- `map_list.py` and `maps.py` include institution-specific mapping functions for metadata **Record** objects.

# Data Layer

- As mentioned above, the Institution class can be instantiated with just an OAI feed URL and a unique identifier, but you can also import a JSON object file that includes an array of objects with the following data.

```
{
  "id": // unique identifier for the institution (required),
  "url": // URL to the OAI endpoint (required),
  "institution": // the full name of the institution (if not set, will default to name set in the 'repositoryName' field in the OAI Identify endpoint),
  "@id_prefix": // an important legacy identifier used by DPLA in certain metadata fields, drawn from previous Heartland Hub ingests. If not set manually, script includes rules for generating one based on id field
  "metadata_prefix": // Preferred metadata prefix for the institution (defaults to oai_dc if none set),
  "include": // An array of specific collection names to include. If present, only collections listed will be crawled.
  "exclude": // An array of specific collection names to exclude. If present, all but these collections will be crawled.
}
```
	
# Run the Pipeline

`python main.py` sets the whole aggregation process in motion for Heartland Hub, first importing the institution data, then crawling OAI feed objects and outputting JSON data for ingest to DPLA

# Experimental Feature(s)

- If you add a new institution that doesn't have a corresponding metadata mapping set, the pipeline will crawl the entire OAI feed, searching for any metadata fields that look like URLs. This is intended to help establish a mapping for schemas where a mapping is not currently known.

# Other scripts and functions

- `init.py` creates necessary files and directories for the pipeline to function properly
- `utils.py` contains helper functions for various transformations in the main classes and functions
- `dpla.py` contains functions to interact with and get data from the DPLA API. It was used during the initial development phase to make sure data was being matched up to previous ingests, but is not used in any of the main crawl functions.
- `get_data.py` similarly is not used in the main process, but was used during the initial building process to compare data from previous Heartland Hub ingests.
- `validate.py` contains function that were used in building the process to compare data. It's not really used.
