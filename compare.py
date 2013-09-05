from pyelasticsearch import ElasticSearch

import networkx as nx

#make connection to elastic search
connection = ElasticSearch('http://demo-n.encodedcc.org:9200')

query = {'query': {'match_all': {}}}

# index with no develops from at all 'ontology'
index = 'ontology_df'
index_compared = 'ontology_cl_df'

slimTerms = {
        'UBERON:0000383': 'musculature of body',
        'UBERON:0000949': 'endocrine system',
        'UBERON:0000990': 'reproductive system',
        'UBERON:0001004': 'respiratory system',
        'UBERON:0001007': 'digestive system',
        'UBERON:0001008': 'excretory system',
        'UBERON:0001009': 'circulatory system',
        'UBERON:0001434': 'skeletal system',
        'UBERON:0002405': 'immune system',
        'UBERON:0002416': 'integumental system',
        'UBERON:0001032': 'sensory system',
        'UBERON:0001017': 'central nervous system',
        'UBERON:0000010': 'peripheral nervous system',
        'UBERON:0002369': 'adrenal gland',
        'UBERON:0002110': 'gallbladder',
        'UBERON:0002106': 'spleen',
        'UBERON:0001173': 'billary tree',
        'UBERON:0001043': 'esophagus',
        'UBERON:0000004': 'nose',
        'UBERON:0000056': 'ureter',
        'UBERON:0000057': 'urethra',
        'UBERON:0000059': 'large intestine',
        'UBERON:0000165': 'mouth',
        'UBERON:0000945': 'stomach',
        'UBERON:0000948': 'heart',
        'UBERON:0000955': 'brain',
        'UBERON:0000970': 'eye',
        'UBERON:0000991': 'gonad',
        'UBERON:0001043': 'esophagus',
        'UBERON:0001255': 'urinary bladder',
        'UBERON:0001264': 'pancreas',
        'UBERON:0001474': 'bone element',
        'UBERON:0002003': 'peripheral nerve',
        'UBERON:0002048': 'lung',
        'UBERON:0002097': 'skin of body',
        'UBERON:0002107': 'liver',
        'UBERON:0000059': 'large intestine',
        'UBERON:0002108': 'small intestine',
        'UBERON:0002113': 'kidney',
        'UBERON:0002240': 'spinal cord',
        'UBERON:0002367': 'prostate gland',
        'UBERON:0002370': 'thymus',
        'UBERON:0003126': 'trachea',
        'UBERON:0001723': 'tongue',
        'UBERON:0001737': 'larynx',
        'UBERON:0006562': 'pharynx',
        'UBERON:0001103': 'diaphragm',
        'UBERON:0002185': 'bronchus',
        'UBERON:0000029': 'lymph node',
        'UBERON:0002391': 'lymph',
        'UBERON:0010133': 'neuroendocrine gland',
        'UBERON:0001132': 'parathyroid gland',
        'UBERON:0002046': 'thyroid gland',
        'UBERON:0001981': 'blood vessel',
        'UBERON:0001473': 'lymphatic vessel',
        'UBERON:0000178': 'blood',
        'UBERON:0002268': 'olfactory organ',
        'UBERON:0007844': 'cartilage element',
        'UBERON:0001690': 'ear',
        'UBERON:0001987': 'placenta',
        'UBERON:0001911': 'mammary gland',
        'UBERON:0001630': 'muscle organ',
        'UBERON:0000007': 'pituitary gland',
        'UBERON:0002370': 'thymus',
        'UBERON:0000478': 'extraembryonic structure'
    }

s = connection.search(query, index=index, size=20000)

results = s['hits']['hits']
terms = []

# Loops compares the terms from different indexes
for result in results:
    try:
        s1 = connection.get(index_compared, 'basic', result['_id'])
        for k in result['_source'].viewkeys() & s1['_source'].viewkeys():
            # Checking for differences between documents between different indexes
            if result['_source'][k] != s1['_source'][k]:
                if k == 'organs' or k == 'systems':
                    # I am worried only about CL terms.
                    if 'CL' in result['_id']:
                        # I don't want any duplicates
                        if result['_source'] not in terms:
                            terms.append(result['_source'])
    except:
        print result['_id']

# Initializes the graph structure and appends edges for each term
G = nx.DiGraph()
for term in results:
    for parent in term['_source']['parents']:
        G.add_edge(term['_source']['id'], parent, r='is_a')
    for part in term['_source']['part_of']:
        G.add_edge(term['_source']['id'], part, r='part_of')
    for d in term['_source']['develops_from']:
        G.add_edge(term['_source']['id'], d, r='develops_from')

slim_terms = []
for slim in slimTerms:
    slim_terms.append(slim)

# Pathetic hack to print out the differences between the terms of 2 indexes
f = open('compare_cl.txt', 'w')

for term in terms:
    # For each term in closure
    for c in term['closure']:
        # Go through the method if the closure term is one of the slim term
        if c in slim_terms:
            # calculates all the possible paths b/w the 2 terms supplied.
            for path in nx.all_simple_paths(G, source=term['id'], target=c):
                rels = []
                uberon_first = 100000
                first = 0
                for p in path:
                    if len(path) - 1 != path.index(p):
                        rels.append(G.get_edge_data(p, path[path.index(p) + 1])['r'])
                    if 'UBERON' in p and uberon_first == 100000:
                        uberon_first = path.index(p)

                for rel in rels:
                    if rel == 'develops_from':
                        first = rels.index(rel)
                        break

                pathidlist = [term['id'],path[first],path[first + 1],path[uberon_first - 1],path[uberon_first],c]
                pathnamelist = []
                for pathid in pathidlist:
                    es = connection.get('ontology_df', 'basic', pathid)
                    pathnamelist.append(es['_source']['name'])

                output = ""
                delimiter = ";"
                for outid, outname in zip(pathidlist, pathnamelist):
                    output = delimiter.join([output,outid,outname])

                f.write(output)
                f.write('\n')
                
                #print term['id'] + '\t' + pathnamelist[0] + '\t' + path[first] + '\t' + pathnamelist[1] + '\t' + path[first + 1] + '\t' + pathnamelist[2] + '\t' + path[uberon_first - 1] + '\t' + pathnamelist[3] + '\t' + path[uberon_first] + '\t' + pathnamelist[4] + '\t' + slimTerms[c] + '\t' + pathnamelist[5]
                break
f.close()