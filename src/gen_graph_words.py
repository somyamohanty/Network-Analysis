import json
import sys
import datetime
import networkx as nx
from bson import json_util
import re
import string
from nltk.stem.snowball import EnglishStemmer

from nltk.corpus import stopwords
from nltk import collocations, pos_tag, word_tokenize
import gensim

cachedStopWords = stopwords.words("english")
import operator

class DocumentFilter():

    def __init__(self, prefixFilter = [], stopWords = [], stem = True):
        self.prefixFilter = prefixFilter
        self.stopWords = stopWords
        self.validChars = string.ascii_letters + string.digits
        self.transTable = string.maketrans(string.punctuation, " " * len(string.punctuation))
        self.stemmer = EnglishStemmer()
        self.stem = stem
            
    def filter(self, doc):
    	doc = gensim.utils.decode_htmlentities(doc)
        try:
            docToken = doc.encode('ascii', errors='ignore')
        except Exception as e:
            print e
            docToken = doc

        docToken = docToken.lower()

        #added POS and Tokenzation. Revert back is docToken = docToken.split() and commment the pos_stuff
        text = word_tokenize(docToken)
        pos_token = pos_tag(text)

        word_types = {'RB', 'JJ', 'VB', 'NN'}
        pos_words = []

        for each in pos_token:
        	if each[1] in word_types:
        		pos_words.append(each[0])

        docToken = pos_words

        docListPrefixless = []
        for w in docToken:
            w = w.strip()
            for c in self.prefixFilter:
                if w.startswith(c) or w in self.stopWords or w in cachedStopWords or len(w) == 1:
                    w = ''
                    break
            if w != '':
            	docListPrefixless.append(w)
         
        docTokenPrefixless = ' '.join(docListPrefixless)
        docTokenPrefixless = docTokenPrefixless.translate(self.transTable)
        docTokenPrefixless = docTokenPrefixless.split()

        docClean = []
        for w in docTokenPrefixless:
        	if len(w) != 1:
        		docClean.append(w)

        return docClean


def read_json(i_file):
	docFilter = DocumentFilter(stem=True, prefixFilter=['http', '@'], stopWords=['rt', 'gt', 'lt', 'amp'])
	with open(i_file, 'r') as f:
		d = []
	        for line in f:
        		t = json.loads(line, object_hook=json_util.object_hook)
        		clean_t = docFilter.filter(t['body'])
        		d.append(clean_t)
        		if len(d) == 50000:
        			break
	return d

def add_node(g, word):
	n = word
	if not g.has_node(n):
		g.add_node(n)
		g.node[n]['weight'] = 1
	else:
		g.node[n]['weight'] += 1

	return g

def add_edge(g, w1, w2, weight):

	if not g.has_edge(w1,w2):
		g.add_edge(w1,w2)
		g[w1][w2]['weight'] = weight
	else:
		g[w1][w2]['weight'] += weight

	return g

def main():
	total = len(sys.argv)

	if total < 1:
		print "Utilization: python gen_graph.py <input_file>"
		exit(0)

	twts = read_json(str(sys.argv[1]))

	collocation = collocations.BigramCollocationFinder.from_documents(twts)

	bigram_measures = collocations.BigramAssocMeasures()


	c_list = []

	for each in collocation.ngram_fd.viewitems():
		if each[1] > 1:
			c_list.append(each)

	c_list.sort(key=operator.itemgetter(1), reverse=True)

	print c_list[:10000]

	# g = nx.Graph()
	
	# for each in c_list[:50000]:
	# 	g = add_node(g, each[0][0])
	# 	g = add_node(g, each[0][1])
	# 	g = add_edge(g, each[0][0], each[0][1], each[1])

	# print len(g)

	# nx.write_graphml(g, '../data/test_graph_words.graphml')



if __name__ == '__main__':
  main()
