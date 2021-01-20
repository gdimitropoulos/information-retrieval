from elasticsearch import Elasticsearch



table=[]
userId = input('PLEASE INSERT YOUR ID : ')
query = input('PLEASE ENTER THE SEARCH WORD :')


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

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])



results = es.search(
    index='movies',
    body=movie_query ,
    size=50
            )


for line1 in results['hits']['hits']:
    results2=es.search(

      index='ratings',
      body={ "query": {  "bool": { "must": [  { "match": {"movieId": line1['_source']['movieId'] }  },
            {   "match": { "userId": userId   } } ]  } } } ,size=50   )

    if not (results2['hits']['hits']):
        val=0.00
    else:
        for line2 in results2['hits']['hits']:
            val=float(line2['_source']['rating'])

    results3=es.search(index='ratings', body={ "query": {  "bool": {  "must": {  "match": {   "movieId":  line1['_source']['movieId']  }  }  } } }, size=25   )

    sum=0.0
    temp=0
    for line3 in results3['hits']['hits']:
         temp+=1
         k=float(line3['_source']['rating'])
         sum+= k
         #print( line3['_source']['rating'])

    if(temp):
        sum=sum/temp
    else:
        sum=0.0
    temp=0
    result=(float(line1['_score'])+sum+val)/3
    line=(line1['_source']['title'],result)

    table.append(line)



table=sorted(table,key=lambda x:x[1],reverse=True)

for i in table:
    print("Title :",i[0]," Score:", "%.2f" % float(i[1]))
