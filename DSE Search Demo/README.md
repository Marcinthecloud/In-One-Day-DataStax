# Solr Amazon Book Demo
This is a quick and dirty demo to get you started with DSE Search. 
With this dataset, you can run geospatial searches, joins, text searches, and faceted searches. 

The dataset includes click stream data, books/categories, and ranks. 

A special thank you to Matt Stump for helping put this together.

#####Prerequisites 
* Python 2.7+
* [DataStax Python Driver](https://github.com/datastax/python-driver) 
* [DataStax Enterprise 4.7.3 or greater](https://www.datastax.com/downloads) 

#####How-to: 
1. Start DataStax Enterprise in search mode
  * ```for tarball installs: bin/dse cassandra -s```
  * ```for package installs: set SOLR=1 in the dse.default file and run: service dse start```
2. Run solr_dataloader.py
  * This will create the CQL schemas and load the data 
3. Run create_core.sh 
  * This will generate Solr cores and index the data
4. Go wild! 

#####Examples Queries: 

```
SELECT * FROM amazon.metadata WHERE solr_query='{"q":"title:Noir~", "fq":"categories:Books", "sort":"title asc"}'  limit 10;

SELECT * FROM amazon.metadata WHERE solr_query='{"q":"title:Noir~", "facet":{"field":"categories"}}'  limit 10;

SELECT * FROM amazon.clicks WHERE solr_query='{"q":"asin:*", "fq":"+{!geofilt pt=\"37.7484,-122.4156\" sfield=location d=1}"}' limit 10;

SELECT * FROM amazon.metadata WHERE solr_query='{"q":"*:*", "fq":"{!join from=asin to=asin force=true fromIndex=amazon.clicks}area_code:415"}' limit 5;

SELECT * FROM amazon.metadata WHERE solr_query='{"q":"*:*", "facet":{"field":"categories"}, "fq":"{!join from=asin to=asin force=true fromIndex=amazon.clicks}area_code:415"}' limit 5;
```
