from pyelasticsearch import ElasticSearch

import networkx as nx

#make connection to elastic search
connection = ElasticSearch('http://localhost:9200')

query = {'query': {'match_all': {}}}

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

s = connection.search(query, index='ontology-new', size=20000)

results = s['hits']['hits']
terms = []

for result in results:
    try:
        s1 = connection.get('ontology', 'basic', result['_id'])
        for k in result['_source'].viewkeys() & s1['_source'].viewkeys():
            if result['_source'][k] != s1['_source'][k]:
                if k == 'organs' or k == 'systems':
                    if 'CL' in result['_id']:
                        if result not in terms:
                            terms.append(result['_source'])
    except:
        print result['_id']

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

for term in terms:
    for c in term['closure']:
        if c in slim_terms:
            for path in nx.all_simple_paths(G, source=term['id'], target=c):
                rels = []
                first = 100000
                uberon_connect = {}
                for p in path:
                    if len(path) - 1 != path.index(p):
                        rels.append(G.get_edge_data(p, path[path.index(p) + 1])['r'])
                    if 'UBERON' in p and first == 100000:
                        first = path.index(p)
                        print first

                for rel in rels:
                    if rel == 'develops_from':
                        first = rels.index(rel)
                        break

                print path
                print rels
                print term['id'] + '\t' + slimTerms[c] + '\t' + path[first] + '\t' + 'develops_from' + '\t' + path[first + 1]
                print
                import pdb;pdb.set_trace();
