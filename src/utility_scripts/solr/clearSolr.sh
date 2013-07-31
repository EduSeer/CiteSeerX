#!/bin/bash

echo "
================================================================================

Clears the index of every Solr instance

================================================================================
"

solr_host="localhost"
solr_port="8080"

# stream.body=<delete><query>*:*</query></delete>&commit=true
curl http://${solr_host}:${solr_port}/solr/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E
curl http://${solr_host}:${solr_port}/solr/update?stream.body=%3Ccommit/%3E

curl http://${solr_host}:${solr_port}/solrAlgorithm/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E

curl http://${solr_host}:${solr_port}/solrAuth/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E

curl http://${solr_host}:${solr_port}/solrPeople/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E

curl http://${solr_host}:${solr_port}/solrTable/update?stream.body=%3Cdelete%3E%3Cquery%3E*:*%3C/query%3E%3C/delete%3E
