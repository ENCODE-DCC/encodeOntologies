from pyelasticsearch import ElasticSearch

#make connection to elastic search
connection = ElasticSearch('http://localhost:9200')

index_old = "ontology"
index_new = "ontology-new"
doc_type_name = "basic"

query = {'query': {'match_all': {}}}

s = connection.search(query, index='ontology', size=20000)

results = s['hits']['hits']

for result in results:
	s1 = connection.get('ontology-new', 'basic', result['_id'])
	for k in result['_source'].viewkeys() & s1['_source'].viewkeys():
		if result['_source'][k] != s1['_source'][k]:
			if k != 'closure':
				print result['_id'] + '(' + result['_source']['name'] + ')'+ ' : ' + str(result['_source'][k])[1:-1] + ' != ' + str(s1['_source'][k])[1:-1]	
