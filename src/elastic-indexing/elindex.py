from datetime import datetime
from elasticsearch import Elasticsearch
import jsonlines
import elasticsearch.helpers
import subprocess

tbeg = datetime.now()

CHUNK_SIZE = 300
MAX_CHUNK_BYTES = 10*1024*1024
INDEX_NAME = "stitched1mil" 
JSON_FILE_NAME = "stitched.json"
THREAD_COUNT = 4
numrecords =  int(subprocess.check_output(['wc','-l',JSON_FILE_NAME]).split()[0]) 

es = Elasticsearch('https://ajit_test:testing123@hpb-search.es.us-east-1.aws.found.io:9243/')

def get_data():
    with jsonlines.open(JSON_FILE_NAME) as reader:
        for item in reader:
            yield item


#jldata = jsonlines.open(JSON_FILE_NAME)
#def get_data():
#    try:
#        return jldata.read()
#    except:
#        return

parallel = True

if parallel:
    for success,errinfo in elasticsearch.helpers.parallel_bulk(es,get_data(),thread_count=THREAD_COUNT,chunk_size=CHUNK_SIZE,max_chunk_bytes=MAX_CHUNK_BYTES,doc_type="book",index=INDEX_NAME):
        if not success:
            print("Failed")
else:
    elasticsearch.helpers.bulk(es,get_data(),doc_type="book",index=INDEX_NAME)

elapsed_time = round((datetime.now()-tbeg).total_seconds(),2)
print("Completed {} records in {} seconds".format(numrecords,elapsed_time))
