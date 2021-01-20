import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.tokenize import word_tokenize
from elasticsearch import Elasticsearch
from sklearn.cluster import KMeans
from sklearn.preprocessing import LabelBinarizer
from gensim.models import Word2Vec
import numpy as np



sum=0.0


es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

movies = es.search( #search movie genres
    index='movies',
    body={
        "query":{
         "match_all":{}}
         },size=9126
            )


gens=['Fantasy', 'Romance', 'Comedy', 'Thriller', 'Children', 'Action',
'Horror', 'Drama', 'Sci-Fi', 'IMAX', 'War', 'Mystery', 'Crime',
'Documentary', 'Musical', 'Western', 'Animation', 'Adventure', 'Film-Noir', '(no genres listed)']




movie_titles=[]
for doc_mov in movies['hits']['hits']:
         movie_titles.append(doc_mov['_source']['title'])


def rmwords(data):
    cachedStopWords = stopwords.words("english")
    removed_stop = []
    for text in data:
        tmp = word_tokenize(text)
        tokens_without_sw = [word for word in tmp if not word in cachedStopWords]
        filtered_sentence = (" ").join(tokens_without_sw)
        removed_stop.append(filtered_sentence)
    return removed_stop





#PART 3 FROM HERE





cluster_Table = [[0.0 for x in range(20)] for y in range(671)]#
gen_table = [[[0.0,0.0] for _ in range(20)] for _ in range(671)]
temp_counter=0
user_counter=1


while(user_counter<671):
    rq1=es.search(
    index='ratings',
    body={
    "query": {
         "match": {
            "userId": user_counter
                        }
                    }
             })

    for doc_rq1 in rq1['hits']['hits']: #for each movie note the ratings

        mq1=es.search(
        index='movies',
        body={ "query": {
                      "match": {
                        "movieId": doc_rq1['_source']['movieId']
                      }
                    }
                    })

        for doc_mq1 in mq1['hits']['hits']:
            txt=doc_mq1['_source']['genres'].split("|")

            for gen_found in txt:
                for cnt,gen_tmp in enumerate(gens):
                    if(gen_tmp==gen_found):
                        tmp=cnt
                        break

                gen_table[user_counter][tmp][0]+=float(doc_rq1['_source']['rating'])#add rating to sum
                gen_table[user_counter][tmp][1]+=1#increase count


    for j in range(20):
        if (gen_table[user_counter][j][1]!=0):
            cluster_Table[user_counter][j] = float(gen_table[user_counter][j][0]/gen_table[user_counter][j][1])

    user_counter+=1

kmeans = KMeans(n_clusters=5)
kmeans.fit(cluster_Table)

for i in range(671):
    for j in range(20):
        if(cluster_Table[i][j]==0):
            cluster_Table[i][j]=kmeans.cluster_centers_[kmeans.labels_[i]][j]



#PART 3 END HERE



movie_titles=rmwords(movie_titles)


movie_data=[]
for cnt,doc_movies in enumerate(movies['hits']['hits']):
    movie_titles[cnt]=[movie_titles[cnt]]
    tmp=(doc_movies['_source']['genres'].split("|"),movie_titles[cnt])
    movie_data.append(tmp)


model = Word2Vec(sentences=movie_titles,size=20, #create vectors
window=5, min_count=1)


lb = LabelBinarizer()
temp= lb.fit_transform(gens)
for data in movie_data:
      onehot_gen=[0.0]*20
      for gen in data[0]:
          cnt=0
          for cont,gen_tmp in enumerate(gens):
              if(gen_tmp==gen):
                  cnt=cont
                  break
          onehot_gen[cnt]=0.5
      model.wv[data[1]]=np.add( model.wv[data[1]],onehot_gen)


userId = int(input('Enter your  id number:'))
Name_of_movie = input('Enter the  query  word :')


pin=[]
rating=[]
BM25_Score=[]
avg_ratings=[]
mq2 = es.search(
    index='movies',
    body={
        "query":{
         "match"
            :{
             "title": Name_of_movie   }}},
            size=100)

for doc_mq2 in mq2['hits']['hits']:
    BM25_Score.append(float(doc_mq2['_score']))
    pin.append(doc_mq2['_source']['title'])


    #PART 2 STARTS HERE
    cnt=0
    sum_rating_movie=0.0
    rq2 = es.search(index='ratings', body={
      "query": {
            "match": {
              "movieId":  doc_mq2['_source']['movieId']
        }
      }
    }, size=100)
    for doc_rq2 in rq2['hits']['hits']:
        cnt+=1
        x=float(doc_rq2['_source']['rating'])
        sum_rating_movie+= x
    if(cnt):
       sum_rating_movie=sum_rating_movie/cnt
    else:
       sum_rating_movie=0.0

           #PART 2 ENDS HERE

#START GUESSING WHAT RATING THE USER CAN SET
    rq3 = es.search(index='ratings', body={
    "query": {
      "bool": {
        "must": [
          {
            "match": {
              "movieId":  doc_mq2['_source']['title']
            }
          },
          {
            "match": {
              "userId": userId
            }
          }
        ]
      }
    }
    }, size=1)
    a=1
    if( not rq3['hits']['hits']):

        x=[doc_mq2['_source']['title']]
        x=rmwords(x)

        sim=model.most_similar(x)
        sum=0.0
        for i in sim:

            mq3 = es.search(index='movies', body={
          "query": {
                "match": {
                  "title": i[0]
                }
              }
            }, size=1)

            for doc_mq3 in mq3['hits']['hits']:
                    rq4 = es.search(index='ratings', body={
                    "query": {
                      "bool": {
                        "must": [
                          {
                            "match": {
                              "movieId":  doc_mq3['_source']['movieId']
                            }
                          },
                          {
                            "match": {
                              "userId": userId
                            }
                          }
                        ]
                      }
                    }
                    }, size=1)
                    if(not rq4['hits']['hits']):
                        tmp=0.0
                        tmp1=doc_mq3['_source']['genres'].split('|')
                        cnt=0
                        for gen in gens:
                            if(gen==tmp1[0]):
                                break
                            cnt+=1
                        tmp=cluster_Table[userId][cnt]
                        sum=sum + tmp
                    else:

                        for doc_rq3 in rq3['hits']['hits']:
                             tmp=float(doc_rq3['_source']['rating'])
                             sum=sum + tmp


        tmp=sum/10
        avg_ratings.append(sum_rating_movie)
        tmp=(tmp+sum_rating_movie )/2
        rating.append(tmp)

    else:
        for doc2_rq3 in  rq3['hits']['hits']:
            rating.append(doc2_rq3['_source']['rating'])


#for e,i in enumerate(rating):
#    print(pin[e],"this is the movie with rating  ",i)


new_similarity_score=[]

for i,e in enumerate(rating):
    tmp=BM25_Score[i]*0.3+ rating[i]*0.5 + avg_ratings[i]*0.2
    new_similarity_score.append(tmp)

final_result=[]
for i,e in enumerate(pin):
    tmp=(pin[i],new_similarity_score[i])
    final_result.append(tmp)




final_result=sorted(final_result ,key=lambda x:x[1],reverse=True)

for i in final_result:
    print("Title : " ,i[0],"Score: " ,  "%.2f" % float(i[1]) )
