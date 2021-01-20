
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
query = input("Insert a keyword:\n")
movie_query={
    "query": {
            "bool": {
              "must": [  {
                  "match": {
                    "title": query
                            }
                        }
                    ]
                }
             }
        }


results = es.search(
        index='movies',body=movie_query,size=50)

for i,result in enumerate(results['hits']['hits']):
    print(i+1, result['_source']['title'],result['_score'],"\n")
