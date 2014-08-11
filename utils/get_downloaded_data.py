import os,sys
import gzip, json
from dateutil import parser
from pymongo import MongoClient

class GnipDataProcessor(object):

    def __init__(self, i_path, collection, chunk_size=50):
        self.path = i_path
        self.chunk = []
        self.chunk_size = chunk_size
        self.collection = collection
        self.total_inserts = 0

    def all_files(self):
        for path, dirs, files in os.walk(self.path):
            for f in files:
                yield os.path.join(path, f)

    def iter_files(self):
        file_generator = self.all_files()

        for f in file_generator:
            try:
                gfile = gzip.open('./'+f)
                for line in gfile:
                    self.process_line(line)
                gfile.close()
            except:
                pass
        if self.chunk != []:
            self.process_chunk()


    def process_line(self, line):
        try:
            if len(self.chunk) > self.chunk_size:
                self.process_chunk()
                self.chunk = []
            if line.strip() != "":
                data = json.loads(line)
                if 'id' in data:
                    data['postedTime_mongo'] = parser.parse(data['postedTime'])
                    self.chunk.append(data)
        except Exception as e:
            print "error storing chunk \n"
            print line, e.msg()
            raise

    def process_chunk(self):
        #for item in self.chunk:
        try:
            self.collection.insert(self.chunk)
            self.total_inserts += len(self.chunk)
            print "Inserted: %d number of docs" % self.total_inserts
        except:
            print "issue inserting"

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]


if __name__ == '__main__':
    total = len(sys.argv)

    if total < 4:
        print "Utilization: python process_downloaded_data.py <input_dir> <mongo_host> <mongo_db> <mongo_collection>"
        exit(0)

    mongo_db = {
        'host' : str(sys.argv[2]),
        'default_port' : 27017,
        'db' : str(sys.argv[3]),
        'collection' : str(sys.argv[4]),
    }
    
    conn = mongo_host(mongo_db)

    insrt = GnipDataProcessor(str(sys.argv[1]),conn.collection, chunk_size=1000)
    insrt.iter_files()
