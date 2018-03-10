## Different Use Cases and PySpark Code to find out desired output

***->***  ***Below is the code to load the data after removing header as first column***

```py
titleBasic_load = sc.textFile("/user/cloudera/input_spark/IMDb_data/title_basic")
header1 = titleBasic_load.first()
titleBasic = titleBasic_load.filter(lambda x : header1 not in x)
  
titleCrew_load = sc.textFile("/user/cloudera/input_spark/IMDb_data/title_crew")
header2 = titleCrew_load.first()
titleCrew = titleCrew_load.filter(lambda x : header2 not in x)

titleEpisode_load = sc.textFile("/user/cloudera/input_spark/IMDb_data/title_episode")
header3 = titleEpisode_load.first()
titleEpisode = titleEpisode_load.filter(lambda x : header3 not in x)

titlePrincipal_load = sc.textFile("/user/cloudera/input_spark/IMDb_data/title_principal")
header4 = titlePrincipal_load.first()
titlePrincipal = titlePrincipal_load.filter(lambda x : header4 not in x)
  
titleRating_load = sc.textFile("/user/cloudera/input_spark/IMDb_data/title_rating")
header5 = titleRating_load.first()
titleRating = titleRating_load.filter(lambda x : header5 not in x)

nameBasic_load = sc.textFile("/user/cloudera/input_spark/IMDb_data/name_basic")
header6 = nameBasic_load.first()
nameBasic = nameBasic_load.filter(lambda x : header6 not in x)
  
 ```
 
 * ### What are the different title types and count their total no and sort it descending order of Count
 
 ```py
 # Only taking title type as a key and assigned value as 1
      
titleBasicMap = titleBasic.map( \
lambda s : (s.split("\t")[1],1))


titleBasicCountByTitle_type = titleBasicMap.reduceByKey(lambda s,t : s+t )

# taking total count of row in titleBasicCountByTitle_type and sorting the data in descending order

titleBasicCountByTitle_typeSorted = titleBasicCountByTitle_type.takeOrdered( \
 titleBasicCountByTitle_type.count(),lambda s :-s[1])

sc.parallelize(titleBasicCountByTitle_typeSorted).map(lambda s : s[0]+str(" ")+str(s[1])).coalesce(1). \
  saveAsTextFile("/user/cloudera/output_spark/IMDb_Output/TitleTypeCount")
           
  ```  
  * ### Find out the top 3 movies with details movie name,genres, rating, director
  
 ```py

titleBasicFilter = titleBasic.filter( \
lambda s : s.split("\t")[1]=="movie")

titleBasicMap = titleBasicFilter.map( \
lambda s : ((s.split("\t")[0]),(s.split("\t")[3],s.split("\t")[8])))

titleRatingMap = titleRating.map( \
lambda s : (s.split("\t")[0],s.split("\t")[1]))

titleJoin = titleBasicMap.join(titleRatingMap)

titlePrincipalMap = titlePrincipal.filter( \
lambda s  : s.split("\t")[3]=="director").map(\
lambda s :(s.split("\t")[0],s.split("\t")[2]))

titlePrincipalJoin = titleJoin.join(titlePrincipalMap)

titlePrincipalJoinMap = titlePrincipalJoin.map(\
lambda t :((t[1][1]),(t[0],t[1][0][0][0],t[1][0][0][1],t[1][0][1])))

nameBasicMap = nameBasic.map( \
lambda s : (s.split("\t")[0],s.split("\t")[1]))

titleJoinBasic = titlePrincipalJoinMap.join(nameBasicMap)

titleJoinBasicMap = titleJoinBasic.map(\
lambda t : (t[1][0],t[1][1]))

# Here grpByKey is used because few movie has multiple directors

titleJoinBasicGrpByTitleNo = titleJoinBasicMap.groupByKey()

finalOutput =titleJoinBasicGrpByTitleNo.map(\
lambda t : (1,(t[0][1],t[0][2],t[0][3],list(t[1])))).groupByKey()

# python UDF to sort the data untill we get highest 3 different ratings data

import itertools as it
def sortingData(p,top):
  sorting = sorted(p[1],key = lambda k : float(k[2]),reverse = True)
  map_sort = map(lambda k : float(k[2]),sorting)
  topNprice = sorted(set(map_sort),reverse=True)[:top]
  return it.takewhile(lambda p : float(p[2]) in topNprice,sorting)

finalOutputSorted = finalOutput.flatMap(\
lambda s :sortingData(s,3))

def concat(s):
  name=""
  for i in range (len(s)):
    if (i>0):
      name = name+","+s[i]
    else:name = s[i]
  return name
  
finalOutputSorted.map(\
lambda s : "name ="+s[0] + " " +"Genres = "+s[1]+" " +"Rating="str(s[2])+" " +"Director="+concat(s[3])).coalesce(1).\
saveAsTextFile("/user/cloudera/output_spark/IMDb_Output/top3Movie")
 
 ```
 * ### Find out the all actress details (name , title type, title name)
 
```py

# using python UDF to fetch  actress details only from dataset

def actress(s) :
  for i in range (len(s.split(","))) :
    if (s.split(",")[i]=="actress"):
      return s
  return "no"
  
nameBasicMapActress = nameBasic.map(\
lambda s: (s.split("\t")[0],s.split("\t")[1],actress(s.split("\t")[4]),s.split("\t")[5])).filter(\
lambda s : s[2]!="no").map(lambda s : (s[0],s[1],s[3]))

# It will break the one row in multiple row because titleType fields consist array of titles seperated by ','
import re
nameBasicFlattenTitles = nameBasicMapActress.map(\
lambda (s) : [(s[0],s[1],mov) for mov in re.split(",+",s[2])]).flatMap(lambda x: x)

nameBasicMap = nameBasicFlattenTitles.map(\
lambda s:(s[2],s[1]))

titleBasicMap = titleBasic.map( \
lambda s : ((s.split("\t")[0]),(s.split("\t")[1],s.split("\t")[3])))

titleJoinName = nameBasicMap.join(titleBasicMap)

titleJoinNameGrpByActress = titleJoinName.map(\
lambda s:(s[1][0],(s[1][0],s[1][1]))).\
groupByKey()

titleJoinNameMap = titleJoinNameGrpByActress.flatMap(\
lambda s : (list(s[1])))

titleJoinNameMap.map(\
lambda s : s[0]+" " +s[1][0]+" "+s[1][1]).coalesce(1).\
saveAsTextFile("/user/cloudera/output_spark/IMDb_Output/actress_details")
 
 ```
 
 * ### Find out the tv Episode,tvSeries,no of Episode,generes only for season no 1 and sort the data on episode no for each tvSeries   

```py
titleBasicEpisode = titleBasic.filter(\
lambda s : s.split("\t")[1]=="tvEpisode").map(\
lambda s : (s.split("\t")[0],(s.split("\t")[3],s.split("\t")[8])))

titleEpisodeMap = titleEpisode.filter(\
lambda s : s.split("\t")[2]!="\N" and int(s.split("\t")[2])==1 ).map(\
lambda s : (s.split("\t")[0],(s.split("\t")[1],s.split("\t")[2],s.split("\t")[3])))

titleEpisodeJoin =titleBasicEpisode.join(titleEpisodeMap)

titleEpisodeJoinMap = titleEpisodeJoin.map(\
lambda t : (t[1][1][0],(t[1][0],t[1][1][2])))

titleBasicSeries  = titleBasic.filter(\
lambda s : s.split("\t")[1]=="tvSeries").map(\
lambda s : (s.split("\t")[0],s.split("\t")[3]))

titleBasicSeriesJoin = titleBasicSeries.join(titleEpisodeJoinMap)

titleBasicSeriesJoinMap = titleBasicSeriesJoin.map(\
lambda t : (t[1][0],(t[1][0],t[1][1][0][0],t[1][1][0][1],t[1][1][1])))

titleBasicSeriesGrpBySeries = titleBasicSeriesJoinMap.groupByKey()

finalResult = titleBasicSeriesGrpBySeries.flatMap( \
lambda s: sorted(s[1],key=lambda k :int(k[3])))
 
finalResult.map(\
lambda s:s[0]+"|"+s[1]+"|"+s[2]+"|"+s[3]+"|").coalesce(1)\
.saveAsTextFile("/user/cloudera/output_spark/IMDb_Output/episode_details")

```

* ### No of Movie,short,video in 1995

```py 
titleBasicFlter = titleBasic.filter(\
lambda s : s.split("\t")[5]!="\N" and int(s.split("\t")[5])==1995 and s.split("\t")[1] in ["movie","short","video"])

titleBasicMap = titleBasicFlter.map(\
lambda s : (s.split("\t")[1],1))

titleBasicMapCountBytitle = titleBasicMap.reduceByKey(lambda s,t : s+t)

titleBasicMapCountBytitleDF = titleBasicMapCountBytitle.toDF(schema=["titletype","count"])
  
titleBasicMapCountBytitleDF.coalesce(1).save("/user/cloudera/output_spark/IMDb_Output/count_1995","json")

```

* ### Find out the below details(Title name,rating,Role of the person) by person "Henry Fonda"

```py 

import re

nameBasicMap = nameBasic.filter(\
lambda s : s.split("\t")[1]=="Henry Fonda").map(\
lambda s : (s.split("\t")[5],s.split("\t")[0],s.split("\t")[1])).map(\
lambda s : [(mov,s[1],s[2]) for mov in re.split(",+",s[0])]).flatMap(lambda x: x).map(\
lambda s : ((s[0],s[1]),s[2]))

nameBasicID = nameBasic.filter(\
lambda s : s.split("\t")[1]=="Henry Fonda").map(\
lambda s : s.split("\t")[0])

t = nameBasicID.first()

titlePrincipalMap = titlePrincipal.filter(\
lambda s : s.split("\t")[2]==t).map(\
lambda s  : ((s.split("\t")[0],s.split("\t")[2]),s.split("\t")[3]))

nameBasicjoin = nameBasicMap.join(titlePrincipalMap)

nameBasicjoinMap = nameBasicjoin.map(\
lambda s : (s[0][0],s[1][1]))

titleMap =titleBasic.map(\
lambda s :(s.split("\t")[0],(s.split("\t")[3],s.split("\t")[1])))

titleMapJoin = nameBasicjoinMap.join(titleMap)

titleJoinMap = titleMapJoin.map(\
lambda s : (s[0],(s[1][1],s[1][0])))

RatingMap = titleRating.map(\
lambda s : (s.split("\t")[0],s.split("\t")[1]))

RatingJoin = titleJoinMap.join(RatingMap)
finalOutput = RatingJoin.map(\
lambda r : (r[1][0][0][0]+"|"+r[1][0][0][1]+"|"+r[1][0][1]+"|"+r[1][1]))

finalOutput.coalesce(1).saveAsTextFile("/user/cloudera/output_spark/IMDb_Output/movieByActor")

```
