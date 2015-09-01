#Simple script to create solr cores and reload/index with the right XML's

#add the path to dsetool if dsetool is not in your path
echo 'Creating Solr cores...'
dsetool create_core amazon.metadata schema=schema_metadata.xml solrconfig=solr_config_metadata.xml reindex=true
dsetool create_core amazon.rank schema=schema_rank.xml solrconfig=solr_config_rank.xml reindex=true
dsetool create_core amazon.clicks schema=schema_clicks.xml solrconfig=solr_config_clicks.xml reindex=true
echo 'finished creating Solr cores!'
