# Onboarding Technical Notes

Some context around the onboarding process as it pertains to the tech portion of things...  
 
Per the request from the group, here are some examples of how the mapping process operates in the context of the ingest pipeline:  
 
- https://github.com/matt-krc/hhub-to-dpla/blob/master/maps.py
- https://github.com/matt-krc/hhub-to-dpla/blob/master/record.py#L54
 
Both of these highlight the kinds of mappings that have to be accounted for on a per-institution basis. While they may not be totally legible to those who aren't familiar with the Python programming language, I hope you can at least kind of get a sense of the logic there.  
 
Specifically, the `maps.py` file contains all the institution-level (or platform-level, in the case of CONTENTdm) mappings to three specific URI fields: `url`, `thumbnail url`, and `IIIF manifest url` that have specific importance to the DPLA data schema. To take the example of CDM (the first function listed here), the url field is always mapped to the last (-1) `identifier` field in the OAI feed, and then that value is fed into two different types of text manipulation functions that extrapolate the urls for the thumbnail and IIIF manifest, since those urls are not directly included in the OAI feed record.  
  
The second link is to a specific function that basically contains all the non-standardized data rules that individual institutions have either specifically requested or other exceptions that have had to be made due to metadata cleaning issues.   

Some examples:  

```python
if institution_id == "mdh" and "publisher" in self.parsed_metadata:
    # Rules for Missouri Digital Heritage
    self.institution = self.parsed_metadata["publisher"][0] + " through Missouri Digital Heritage"
```    
This basically stipulates that Missouri Digital Heritage records include 'through Missouri Digital Heritage' after the data provider, since that isn't provided explicitly anywhere in the metadata feed.  

Similarly, Springfield Green County Library had a specific collection that needed a different rights statement from the rest of the collections (which wasn't included directly in the OAI feed), so an exception had to be added for that collection:  

```python
elif institution_id == "sgcl":
    collection = self.parsed_header["identifier"][0].split(":")[-1].split("/")[0]
    if collection == "p16792coll1":
        rights = "The Ozarks Genealogical Society, Inc. offers access to this collection for " \
                             "educational and personal research purposes only.  Materials within the collection " \
                             "may be protected by the U.S. Copyright Law (Title 17, U.S.C.).  It is the " \
                             "researcher's obligation to determine and satisfy copyright or other use restriction " \
                             "when publishing or otherwise distributing materials within the collection."
```

Other institutions have other metadata-level exceptions here that had to be included that I could explain in more detail if need be.  
 
I hope this helps give additional context for some of the mapping considerations that take place when onboarding. Please feel free to reach out with any additional questions or concerns. Also, I welcome any ideas about currently existing standards that could help solve some of these issues. 
 
 