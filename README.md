elasticsearch
=============

Python module downloads Uberon, Cell Ontology and OBI ontology and parses into a dictionary.
At the end entire dictionary is indexed in Elastic Search

Requirements:

1. pyelasticsearch package - https://github.com/rhec/pyelasticsearch
2. Elastic Search should be installed on localhost or on different server (edit module accordingly)
3. Run following commands to generate the mapping for the 'index' and 'doc_type' in elastic search

Following command sets the analyzer for description
<code>
curl -XPOST 'http://localhost:9200/ontology/?pretty=1' -d '
{
  "settings": {
    "analysis": {
      "analyzer": {
        "suggestions": {
          "tokenizer": "standard",
          "filter": ["suggestions_shingle"]
        }
      },
      "filter": {
        "suggestions_shingle": {
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 5
        }
      }
    }
  }
}'
</code>

Following command sets multiple field so that we can do shingle tokens
<code>
curl -XPUT 'http://localhost:9200/ontology/basic/_mapping?pretty=1' -d '
{
  "basic": {
    "properties": {
      "description": {
        "type": "multi_field",
        "fields": {
          "name": { "type": "string", "analyzer": "standard", "include_in_all": true },
          "suggestions": { "type": "string", "analyzer": "suggestions", "include_in_all": false }
        }
      }
    }
  }
}
</code>

Known Issues:

1. Few Go Terms do not have 'name' attribute, for now they are ignored. Should be fixed in later versions
2. Script uses merged ontology files so there are duplciates

Sample query to search
<code>
curl -XGET 'http://localhost:9200/ontology/basic/_search?pretty=1' -d '{
  "query":{
    "prefix":{
      "description.suggestions":"main entity"
    }
  },
  "facets":{
    "description_suggestions":{
      "terms":{
        "field":"description.suggestions",
        "regex":"^main entity.*",
        "size": 10
      }
    }
  }
}
'
</code>
