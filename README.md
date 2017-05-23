encodeOntologies
=============

This module downloads Uberon (merged) ontology and parses into a dictionary.
At the end entire dictionary is indexed in Elastic Search.

For each term a closure is calculated by traversing the 'is_a', 'part_of' and 'develops_from' relationships to the root.
Each term is also mapped to appropriate 'Organ System slim' and  'organ slim'.

Requirements:

1. pyelasticsearch package - https://github.com/rhec/pyelasticsearch
2. NetworkX package - http://networkx.github.io/
3. Elastic Search should be installed on localhost or on different server (edit module accordingly)

