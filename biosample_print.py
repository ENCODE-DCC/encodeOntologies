from pyelasticsearch import ElasticSearch

#make connection to elastic search
connection = ElasticSearch('http://demo-n.encodedcc.org:9200')

query = {'query': {'match_all': {}}, 'fields': ['accession', 'biosample_term_name', 'biosample_term_id', 'organ_slims', 'system_slims']}

# index with no develops from at all 'ontology'
index = 'biosamples'

s = connection.search(query, index=index, size=1000)
for data in s['hits']['hits']:
    dataS = data['fields']
    print dataS['accession'] + '\t' + dataS['biosample_term_id'] + '\t' + dataS['biosample_term_name'] + '\t' + ', '.join(dataS['organ_slims']) + '\t' + ', '.join(dataS['system_slims'])
