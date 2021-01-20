import csv
from elasticsearch import Elasticsearch
from elasticsearch import helpers


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#es.indices.delete(index='movies', ignore=[400, 404])
print(es.ping())

def convert(filename,indexname,type):
    with open(filename, encoding="utf8") as file:
        r = csv.DictReader(file)
        helpers.bulk(es, r, index=indexname, doc_type=type)
es.indices.create(index = 'movies',ignore=400,body={
    "settings":{
        "index":{
            "similarity":{
                "default":{
                    "type":"BM25"
                }
            }
        }
    }})
es.indices.create(index = 'ratings',ignore=400,body={
    "settings":{
        "index":{
            "similarity":{
                "default":{
                    "type":"BM25"
                }
            }
        }
    }})
convert('movies.csv','movies','movies')
convert('ratings.csv','ratings','ratings')
print(es.indices.exists('movies'))
print(es.indices.exists('ratings'))
