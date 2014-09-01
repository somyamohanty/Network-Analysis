import json
import sys
import datetime
import networkx as nx
from bson import json_util
import re
from networkx.readwrite import json_graph

def main():
	total = len(sys.argv)
	if total < 1:
		print "Utilization: python conv_graphml_graphjson.py <input_file> <output_file>"
		exit(0)

	G = nx.read_graphml(str(sys.argv[1]))

	with open(str(sys.argv[2]), 'w') as outfile:
		outfile.write(json.dumps(json_graph.node_link_data(G)))


if __name__ == '__main__':
	main()

