# Module downloads, parses all ontology files and indexes them in Elastic Search
# Note: This is the basic version
from pyelasticsearch import *
from urllib2 import Request, urlopen, URLError, HTTPError

uberonURL = "http://purl.obolibrary.org/obo/uberon.obo"
cellOntologyURL = "http://purl.obolibrary.org/obo/cl.obo"
obiURL = "http://purl.obolibrary.org/obo/obi/obi.obo"

urls = [uberonURL, cellOntologyURL, obiURL]

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
        children = terms[goid]['c']
        if len(children) > 0:
            for child in children:
                recursiveArray.extend(getDescendents(child))

    return set(recursiveArray)


def getAncestors(goid):
    ''' Get Ancestors of GO ID '''

    recursiveArray = [goid]
    if terms.has_key(goid):
        parents = terms[goid]['p']
        if len(parents) > 0:
            for parent in parents:
                recursiveArray.extend(getAncestors(parent))

    return set(recursiveArray)


#counter for indexing this is used as ID in elastic search
counter = 0

print "Starting...."
print

terms = {}

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
            try:
                termID = term['id'][0]
                termName = term['name'][0]

                #only add to the structure if the term has a is_a tag
                #the is_a value contain GOID and term definition
                #we only want the GOID
                if term.has_key('is_a'):
                    termParents = [p.split()[0] for p in term['is_a']]

                    if not terms.has_key(termID):
                        #each goid will have two arrays of parents and children
                        terms[termID] = {'termID':'', 'description': '',
                         'parents':[], 'children':[]}

                    #append termID and termName to the dict
                    terms[termID]['termID'] = termID
                    terms[termID]['description'] = termName 

                    #append parents of the current term
                    terms[termID]['parents'] = termParents

                    #for every parent term, add this current term as children
                    for termParent in termParents:
                        if not terms.has_key(termParent):
                            terms[termParent] = {'parents':[], 'children':[]}
                        terms[termParent]['children'].append(termID)
            except KeyError:
                print "Term doesn't have the name - " + termID
        else:
            print 
            break

print "Indexing the data in ElasticSearch..."
for term in terms:
    counter += 1
    connection.index(index_name, doc_type_name, terms[term], id=counter)
    connection.refresh()

print
print "Total GO Terms indexed " + str(counter)
