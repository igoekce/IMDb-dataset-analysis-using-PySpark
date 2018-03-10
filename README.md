# IMDb-dataset-using-Pyspark

Analysis of  different titles(movie,short,game,tvSeries,tvEpisode e.t.c), actor,actress,director details,rating of the titles from IMDb site using PySpark core API

## IMDb dataset details and fields details of corresponding files in datset
Total size of the datset is `2.21GB`

Each dataset is contained in a `tab-separated-values (TSV) formatted` file in the UTF-8 character set. The **first line in each file contains headers** that describe what is in each column. A ‘\N’ is used to denote that a particular field is missing or null for that title/name. The available datasets are as follows: 

  #### `title_basics`  : 
  Contains the title information
  
  * tconst (string) - alphanumeric unique identifier of the title
  * titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
  * primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release
  * originalTitle (string) - original title, in the original language
  * isAdult (boolean) - 0: non-adult title; 1: adult title.
  * startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year.
  * endYear (YYYY) – TV Series end year. ‘\N’ for all other title types
  * runtimeMinutes – primary runtime of the title, in minutes
  * genres (string array) – includes up to three genres associated with the title
  
  #### `title_episode` : 
  Contains  the TvEpisode information
  
  * tconst (string) - alphanumeric identifier of episode
  * parentTconst (string) - alphanumeric identifier of the parent TV Series
  * seasonNumber (integer) – season number the episode belongs to
  * episodeNumber (integer) – episode number of the tconst in the TV series
  
  #### `title_ratings` : 
  Contains IMDb rating and vote details for all titles(tconst)
  
  * tconst (string)
  * averageRating – weighted average of all the individual user ratings
  * numVotes - number of votes the title has received
  
  #### `title_principals` :
   Contains the  cast for titles
   
   * tconst (string) - alphanumeric unique identifier of the title
   * ordering (integer) - ordering of title
   * nconst (string) - alphanumeric unique identifier of the name/person
   
  #### `name_basics` :
   Contains the following information for names
   
   * nconst (string) - alphanumeric unique identifier of the name/person
   * primaryName (string)– name by which the person is most often credited
   * birthYear – in YYYY format
   * deathYear – in YYYY format if applicable, else ‘\N’
   * primaryProfession (array of strings)– the top-3 professions of the person
   * knownForTitles (array of tconsts) – titles the person is known for

  #### `title_crew` : 
   Contains the director and writer information
   
  * tconst (string)
  * directors (array of nconsts) - director(s) of the given title
  * writers (array of nconsts) – writer(s) of the given title
  

   
   
   
