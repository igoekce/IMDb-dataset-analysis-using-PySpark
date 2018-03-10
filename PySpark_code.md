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

# taking total data in titleBasicCountByTitle_type and sorting the data in descending order

titleBasicCountByTitle_typeSorted = titleBasicCountByTitle_type.takeOrdered( \
 titleBasicCountByTitle_type.count(),lambda s :-s[1])

sc.parallelize(titleBasicCountByTitle_typeSorted).map(lambda s : s[0]+str(" ")+str(s[1])).coalesce(1). \
  saveAsTextFile("/user/cloudera/output_spark/IMDb_Output/TitleTypeCount")
           
  ```     
