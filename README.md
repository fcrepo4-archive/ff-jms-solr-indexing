----
# This project is no longer maintained
----

PoC for a Solr indexing client listening on a JMS Queue
=======================================================

Installation
------------

1. Download and Install Solr from http://lucene.apache.org/solr/
2. Change the configuration so that a new Searcher is created on every commit, so changes made to the index are visible immediately in e.g. $SOLR_HOME/collection1/conf/solrconfig.xml change  the autocommit part to include `<openSearcher>true</openSearcher>`
