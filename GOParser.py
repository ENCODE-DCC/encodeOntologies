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


def getRelatedTerms(goid):
    ''' Get relations of GO ID'''

    recursiveArray = [goid]
    if terms.has_key(goid):
        relations = terms[goid]['data']
        if len(relations) > 0:
            for relation in relations:
                recursiveArray.extend(getRelatedTerms(relation))

    return set(recursiveArray)


def getSystemSlims(goid):
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


def getOrganSlims(goid):
    ''' Get Organ Slims '''

    slims = []
    slimTerms = {
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
        'UBERON:0001255': 'urinay bladder',
        'UBERON:0001264': 'pancreas',
        'UBERON:0001474': 'bone element',
        'UBERON:0002003': 'peripheral nerve',
        'UBERON:0002048': 'lung',
        'UBERON:0002097': 'skin of body',
        'UBERON:0002107': 'liver',
        'UBERON:0002108': 'small intestine',
        'UBERON:0002113': 'kidney',
        'UBERON:0002240': 'spinal cord',
        'UBERON:0002367': 'prostate gland',
        'UBERON:0002370': 'thymus',
        'UBERON:0003126': 'trachea'
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
term_queue = {}
# UBERON has 2 Roots god knows why, so one of the roots is entered manually here
terms['UBERON:0001062'] = {'id': 'UBERON:0001062', 'name': 'anatomical entity', 'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'overlaps': [], 'closure': [], 'data': []}
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
                    terms[term['id'][0]] = {'id': '', 'name': '', 'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'organs': [], 'closure': [], 'slims': [], 'data': []}
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
                                terms[termID] = {'id': '', 'name': '', 'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'organs': [], 'closure': [], 'slims': [], 'data': []}

                            #append termID and termName to the dict
                            terms[termID]['id'] = termID
                            terms[termID]['name'] = termName

                            #append parents of the current term
                            terms[termID]['parents'] = termParents

                            #for every parent term, add this current term as children
                            for termParent in termParents:
                                if not terms.has_key(termParent):
                                    terms[termParent] = {'parents': [], 'children': [], 'part_of': [], 'develops_from': [], 'organs': [], 'closure': [], 'slims': [], 'data': []}
                                terms[termParent]['children'].append(termID)
                            if term.has_key('relationship'):
                                relations = [p.split()[0] for p in term['relationship']]
                                relationTerms = [p.split()[1] for p in term['relationship']]
                                count = 0
                                relationships = ['part_of', 'develops_from']
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
    terms[term]['data'] = list(set(terms[term]['parents']) | set(terms[term]['part_of']) | set(terms[term]['develops_from']))


for term in terms:
    for t in terms[term]['data']:
        words = getRelatedTerms(t)
        for word in words:
            terms[term]['closure'].append(word)

    terms[term]['closure'] = list(set(terms[term]['closure']))
    terms[term]['systems'] = getSystemSlims(term)
    terms[term]['organs'] = getOrganSlims(term)


count = 0
for term in terms:
    del(terms[term]['children'])
    del(terms[term]['part_of'])
    del(terms[term]['develops_from'])
    del(terms[term]['parents'])
    del(terms[term]['data'])
    # import pdb; pdb.set_trace();
    connection.index(index_name, doc_type_name, terms[term], id=count)
    connection.refresh()
    count = count + 1

print
print "Total GO Terms indexed " + str(count)
