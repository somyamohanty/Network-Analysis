import json
import sys
import datetime
import networkx as nx
from bson import json_util
import re

class Mention_user(object):
	def __init__(self, twt):
		self.id = twt['id']
		self.screen_name = twt['screen_name']
		self.display_name = twt['display_name']
	
class Twt_user(object):
	def __init__(self, twt):
		self.id = str(twt['actor']['id']).split(":")[2]
		self.screen_name = twt['actor']['preferredUsername']
		self.display_name = twt['actor']['displayName']
		self.followers = twt['actor']['followersCount']
		self.friends = twt['actor']['friendsCount']
		self.mentions = twt['twitter_entities']['user_mentions']
		self.rtweets = twt['retweetCount']
		if 'klout_score' in twt['gnip']:
			self.klout = twt['gnip']['klout_score']
		else:
			self.klout = 0

	def get_user_mentions(self):
		for each in self.mentions:
			mention = {
				'id': each['id'],
				'screen_name' : each['screen_name'],
				'display_name' :each['name']
			}
			yield mention

def filter_tweets(t):
	words = ['hurricane', 'sandy', 'power', 'evacuation','flooding', 'rain', 'weather', 'safe', 'help', 'bad', 'outside', 'frakenstorm' , 'volunteer', 'stay', 'wind', 'omg', 'watch', 'tomorrow']
	for each in words:
		if re.search(each, t, re.IGNORECASE):
			return True
	return False

def read_json(i_file):
	with open(i_file, 'r') as f:
		d = []
	        for line in f:
        		t = json.loads(line, object_hook=json_util.object_hook)
        		d.append(t)
        		# if filter_tweets(t['body']):
        		# 	d.append(t)
	return d

def add_node(g, twt_user):
	n = twt_user.id
	if not g.has_node(n):
		g.add_node(n)
		g.node[n]['weight'] = 1
		g.node[n]['screen_name'] = twt_user.screen_name
	else:
		g.node[n]['weight'] += 1

	return g

def add_edge(g, n1, n2, e_type):

	if not g.has_edge(n1.id,n2.id):
		g.add_edge(n1.id,n2.id)
		g[n1.id][n2.id]['weight'] = 1
		g[n1.id][n2.id]['type'] = e_type
	else:
		g[n1.id][n2.id]['weight'] += 1

	return g

def main():
	total = len(sys.argv)

	if total < 1:
		print "Utilization: python gen_graph.py <input_file>"
		exit(0)

	twts = read_json(str(sys.argv[1]))

	g = nx.DiGraph()

	count = 0
	for each in twts:
		usr = Twt_user(each)
		g = add_node(g, usr)
		for mention in usr.get_user_mentions():
			m_usr = Mention_user(mention)
			g = add_node(g, m_usr)
			g = add_edge(g, usr, m_usr, 'mention')
		count +=1

	print len(g)

	# removing singleton pairs
	for each in g.edges():
		if g.degree(each[0]) == g.degree(each[1]) == 1:
			g.remove_node(each[0])

	#remove single nodes
	for each in g.nodes():
		if g.degree(each) < 5:
			g.remove_node(each)
	
	nx.write_graphml(g, '../data/test_graph_di_kw_2.graphml')

	print "total twts: %d" % count




if __name__ == '__main__':
  main()
