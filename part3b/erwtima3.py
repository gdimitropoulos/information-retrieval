
from elasticsearch import Elasticsearch
from sklearn.cluster import KMeans


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])



genres=['Fantasy', 'Romance', 'Comedy', 'Thriller', 'Children', 'Action',
'Horror', 'Drama', 'Sci-Fi', 'IMAX', 'War', 'Mystery', 'Crime',
'Documentary', 'Musical', 'Western', 'Animation', 'Adventure', 'Film-Noir', '(no genres listed)']

cluster_Table = [[0.0 for x in range(20)] for y in range(671)]#
gen_table = [[[0.0,0.0] for _ in range(20)] for _ in range(671)]
temp_counter=0
user_counter=1 # for each user make a list of average genre ratings


while(user_counter<671):
    rq1=es.search(  index='ratings', body={ "query": {  "match": {   "userId": user_counter   }  }  })

    for doc_rq1 in rq1['hits']['hits']:
        mq1=es.search(  index='movies', body={ "query": {  "match": {  "movieId": doc_rq1['_source']['movieId']   } } })

        for doc_mq1 in mq1['hits']['hits']:
            txt=doc_mq1['_source']['genres'].split("|")

            for gen_found in txt:
                for cnt,gen_tmp in enumerate(genres):
                    if(gen_tmp==gen_found):
                        tmp=cnt
                        break

                gen_table[user_counter][tmp][0]+=float(doc_rq1['_source']['rating'])#add rating to sum
                gen_table[user_counter][tmp][1]+=1#increase count


    for j in range(20):
        if (gen_table[user_counter][j][1]!=0):
            cluster_Table[user_counter][j] = float(gen_table[user_counter][j][0]/gen_table[user_counter][j][1])
    print(cluster_Table[user_counter][1])
    user_counter+=1




kmeans = KMeans(n_clusters=5)
kmeans.fit(cluster_Table)



for i in range(671):
    for j in range(20):
        if(cluster_Table[i][j]==0):
            cluster_Table[i][j]=kmeans.cluster_centers_[kmeans.labels_[i]][j]

# #Search for movie titles containing the keyword

Name_of_user = input('Insert The Title Of The Movie: ')
Number_of_user=int(input("Insert user id "))




sum=0.0

movie_query_body = {"query": {  "match": {   "title": Name_of_user   } } }



mq2 = es.search(index='movies', body=movie_query_body, size=50)
cnt=0
pin=[]
for doc_mq2 in mq2['hits']['hits']:
     print( doc_mq2['_source'])
     rq2 = es.search(index='ratings', body={ "query": { "bool": { "must": [ {  "match": { "movieId": doc_mq2['_source']['movieId']  }},
     { "match": { "userId": Number_of_user }}] }
       }
     }, size=100)
     if(rq2['hits']['hits']):
         for doc_rq2 in rq2['hits']['hits']:
              tmp=float(doc_rq2['_source']['rating'])
     else:
        tmp=0.0
        tmp1=doc_mq2['_source']['genres'].split('|')

        cnt=0
        for gen_type in genres:
            if(gen_type==tmp1[0]):
                break
            cnt+=1

        tmp=cluster_Table[Number_of_user][cnt]


     cnt=0
     sum=0.0
     rq3 = es.search(index='ratings', body={
       "query": {
             "match": {
               "movieId":  doc_mq2['_source']['movieId']
             }
       }
     }, size=100)



     for doc_rq3 in rq3['hits']['hits']:
         cnt+=1
         sum+= float(doc_rq3['_source']['rating'])


     if(cnt):
        sum=sum/cnt
     else:
        sum=0.0

     cnt=0
     result=(float(doc_mq2['_score'])*0.2+sum*0.2+tmp*0.6)
     y=(doc_mq2['_source']['title'],result)
     pin.append(y)


table=sorted(pin ,key=lambda x:x[1],reverse=True)
for i in table:
    print("Title :",i[0]," Score:", "%.2f" % float(i[1]))
