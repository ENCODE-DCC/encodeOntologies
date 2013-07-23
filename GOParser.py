# Module downloads, parses all ontology files and indexes them in Elastic Search
# Note: This is the basic version
from pyelasticsearch import ElasticSearch
from urllib2 import Request, urlopen


uberonURL = "http://purl.obolibrary.org/obo/uberon.obo"
cellOntologyURL = "http://purl.obolibrary.org/obo/cl.obo"
obiURL = "http://purl.obolibrary.org/obo/obi/obi.obo"

urls = [uberonURL]

#make connection to elastic search
connection = ElasticSearch('http://localhost:9200')

index_name = "ontology"
doc_type_name = "basic"


def getTerm(stream):
    ''' Get GO term block from the obo file'''

    block = []
    for line in stream:
        if line.strip() == "[Term]" or line.strip() == "[Typedef]":
            break
        else:
            if line.strip() != "":
                block.append(line.strip())

    return block


def parseTagValue(term):
    ''' Get tag-value pairs of each GO Term '''

    data = {}
    for line in term:
        tag = line.split(': ', 1)[0]
        value = line.split(': ', 1)[1]
        if not data.has_key(tag):
            data[tag] = []
        data[tag].append(value)

    return data


def getDescendents(goid):
    ''' Get Descendents of Go Term '''

    recursiveArray = [goid]
    if terms.has_key(goid):
        children = terms[goid]['children']
        if len(children) > 0:
            for child in children:
                recursiveArray.extend(getDescendents(child))

    return set(recursiveArray)


def getAncestors(goid):
    ''' Get Ancestors of GO ID '''

    recursiveArray = [goid]
    if terms.has_key(goid):
        parents = terms[goid]['parents']
        if len(parents) > 0:
            for parent in parents:
                recursiveArray.extend(getAncestors(parent))

    return set(recursiveArray)


def getRelatedTerms(goid, relationshipType):
    ''' Get relations of GO ID'''

    recursiveArray = [goid]
    if terms.has_key(goid):
        relations = terms[goid][relationshipType]
        if len(relations) > 0:
            for relation in relations:
                recursiveArray.extend(getRelatedTerms(relation, relationshipType))

    return set(recursiveArray)


def getSlims(goid):
    ''' Get Slims '''

    slims = []
    slimTerms = {
        'UBERON:0000383': 'musculature of body',
        'UBERON:0000949': 'endocrine system',
        'UBERON:0000990': 'reproductive system',
        'UBERON:0001004': 'respiratory system',
        'UBERON:0001007': 'digestive system',
        'UBERON:0001008': 'excretory system',
        'UBERON:0001009': 'circulatory system',
        'UBERON:0001016': 'nervous system',
        'UBERON:0001434': 'skeletal system',
        'UBERON:0002405': 'immune system',
        'UBERON:0002416': 'integumental system'
    }
    for slimTerm in slimTerms:
        if slimTerm in terms[term]['closure']:
            slims.append(slimTerms[slimTerm])
    return slims

#counter for indexing this is used as ID in elastic search
counter = 0

print "Starting...."
print

terms = {}
# UBERON has 2 Roots god knows why, so one of the roots is entered manually here
terms['UBERON:0001062'] = {'id': 'UBERON:0001062', 'name': 'anatomical entity', 'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'overlaps': [], 'closure': []}
stupid_terms = []

for url in urls:

    req = Request(url)

    print "downloading - " + url
    oboFile = urlopen(req)

    #skip the file header lines
    print "parsing ontology - " + url
    getTerm(oboFile)

    #infinite loop to go through the obo file.
    #Breaks when the term returned is empty, indicating end of file
    while 1:
        #get the term using the two parsing functions
        term = parseTagValue(getTerm(oboFile))
        if len(term) != 0:
            if (term['id'][0]).find(':') != -1:
                # Handling root terms
                if term['id'][0] == 'UBERON:0000000':
                    terms[term['id'][0]] = {'id': '', 'name': '', 'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'overlaps': [], 'closure': [], 'slims': []}
                    terms[term['id'][0]]['id'] = term['id'][0]
                    terms[term['id'][0]]['name'] = term['name'][0]
                else:
                    try:
                        termID = term['id'][0]
                        termName = term['name'][0]

                        if term.has_key('is_a'):
                            termParents = [p.split()[0] for p in term['is_a']]

                            if not terms.has_key(termID):
                                #each goid will have two arrays of parents and children
                                terms[termID] = {'id': '', 'name': '', 'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'overlaps': [], 'closure': [], 'slims': []}

                            #append termID and termName to the dict
                            terms[termID]['id'] = termID
                            terms[termID]['name'] = termName

                            #append parents of the current term
                            terms[termID]['parents'] = termParents

                            #for every parent term, add this current term as children
                            for termParent in termParents:
                                if not terms.has_key(termParent):
                                    terms[termParent] = {'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'overlaps': [], 'closure': [], 'slims': []}
                                terms[termParent]['children'].append(termID)
                            if term.has_key('relationship'):
                                relations = [p.split()[0] for p in term['relationship']]
                                relationTerms = [p.split()[1] for p in term['relationship']]
                                count = 0
                                relationships = ['part_of', 'develops_from', 'overlaps']
                                for relation in relations:
                                    if relation in relationships:
                                        terms[termID][relation].append(relationTerms[count])
                                    count = count + 1

                    except KeyError:
                        stupid_terms.append(termID)
        else:
            break

# Deleting all useless, time wasting terms
for stupid_term in stupid_terms:
    del(terms[stupid_term])

useless = []
for term in terms:
    try:
        terms[term]['id']
    except:
        useless.append(term)

for useL in useless:
    del(terms[useL])

print "Take a break, I have to calculate closures for " + str(len(terms)) + " ontology terms and index them in elasticsearch. Long operation. Sigh!!"

for term in terms:

    # Get relationship terms and append to closure
    rels = getRelatedTerms(term, 'part_of')
    for rel in rels:
        words1 = getAncestors(rel)
        words2 = getRelatedTerms(rel, 'part_of')
        words3 = getRelatedTerms(rel, 'develops_from')
        words4 = getRelatedTerms(rel, 'overlaps')
        words = words1 | words2 | words3 | words4
        for word in words:
            terms[term]['closure'].append(word)

    terms[term]['closure'] = list(set(terms[term]['closure']))
    devs = getRelatedTerms(term, 'develops_from')
    for dev in devs:
        words1 = getAncestors(dev)
        words2 = getRelatedTerms(dev, 'part_of')
        words3 = getRelatedTerms(dev, 'develops_from')
        words4 = getRelatedTerms(dev, 'overlaps')
        words = words1 | words2 | words3 | words4
        for words in words:
            terms[term]['closure'].append(dev)

    terms[term]['closure'] = list(set(terms[term]['closure']))
    overs = getRelatedTerms(term, 'overlaps')
    for over in overs:
        words1 = getAncestors(over)
        words2 = getRelatedTerms(over, 'part_of')
        words3 = getRelatedTerms(over, 'develops_from')
        words4 = getRelatedTerms(over, 'overlaps')
        words = words1 | words2 | words3 | words4
        terms[term]['closure'].append(over)

    terms[term]['closure'] = list(set(terms[term]['closure']))
    ancestors = getAncestors(terms[term]['id'])
    for ancestor in ancestors:
        terms[term]['closure'].append(ancestor)
        nodes1 = getRelatedTerms(ancestor, 'part_of')
        nodes2 = getRelatedTerms(ancestor, 'develops_from')
        nodes3 = getRelatedTerms(ancestor, 'overlaps')
        nodes = nodes1 | nodes2 | nodes3
        for node in nodes:
            terms[term]['closure'].append(node)

    terms[term]['closure'] = list(set(terms[term]['closure']))
    terms[term]['closure'].remove(term)
    terms[term]['slims'] = getSlims(term)

count = 0
for term in terms:
    del(terms[term]['children'])
    del(terms[term]['part_of'])
    del(terms[term]['develops_from'])
    del(terms[term]['overlaps'])
    del(terms[term]['parents'])
    # import pdb; pdb.set_trace();
    connection.index(index_name, doc_type_name, terms[term], id=count)
    connection.refresh()
    count = count + 1

print
print "Total GO Terms indexed " + str(count)
