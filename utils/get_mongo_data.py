from pymongo import MongoClient
import json
import sys
import datetime
from bson import json_util

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]
        self.query_field = mongo_db['query_field']
        self.output_field = mongo_db['output_field']

    def get_data(self):
    	start = datetime.datetime(2012, 10, 28)
    	end = datetime.datetime(2012, 10, 30)

        try:
            for doc in self.collection.find({'postedTime': {'$gte': start, '$lte': end}}):
				yield doc
	except Exception as e:
		print e
		pass


def main():
	total = len(sys.argv)

	if total < 5:
		print "Utilization: python get_mongo_data.py <mongo_host> <mongo_db> <mongo_collection> <query_field> <output_field> <output_file>"
		exit(0)

	mongo_db = {
		'host' : str(sys.argv[1]),
		'default_port' : 27017,
		'db' : str(sys.argv[2]),
		'collection' : str(sys.argv[3]),
		'query_field' : str(sys.argv[4]),
		'output_field' : str(sys.argv[5]),
	}

	conn = mongo_host(mongo_db)

	count = 1

	count_docs = 1

	with open(sys.argv[6], 'a') as jfile:
	    for result in conn.get_data():
	        # print result
	        count_docs += 1
	        if result != {}:
	        	json.dump(result,jfile,default=json_util.default)
	        	jfile.write('\n')
		        count += 1
		        if count % 500 == 0:
		            print "Found and wrote: %d out of %d total docs" % (count, count_docs)

if __name__ == '__main__':
    main()
