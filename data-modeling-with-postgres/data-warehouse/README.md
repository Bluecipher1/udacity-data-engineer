# Data warehouse
This directory contains a solution to the third project of Udacity's *Data Engineer* Nanodegree program. It describes the workflow to create a data warehouse for Sparkify, a fictitious music streamimg app. The data set being used (not contained in this folder) is a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). It is provided in S3 Buckets on AWS.

## Usage

### Requirements
The implementation is based on Python 3 which needs to be installed locally. It also requires the *psycopg2* module available for import. The song dataset is loaded from several JSON files that reside in S3 buckets into staging tables in an Amazon Redshift cluster. The data is then transformed and loaded into a star schema in the Redshift database. You therefore need an Redshift cluster that has read-only access to S3 and allows for TCP transport on port 5439. The cluster's configuration data must be specified in dwh.cfg.

### Execution
The datwarehouse creation is done in several steps implemented in two Python scripts:
```
python create_table.py
```
This script loads all SQL queries specified in sql_queries.py and creates the required database schema.
```
python etl.py
```
This ETL script first loads the data from all event and song files in S3 into staging tables using Redshift's COPY command. It then loads the data into the star schema, with songplays being the fact table and songs, artists, users and times being the corresponding dimension tables.

## Why is the data moved to a database?
The Sparkify data analystics team would like to analyze the listening behaviour od their clients. This includes questions like
* What songs are users listening to?
* How long do they listen to a song on average?
* What are the top 10 favourite songs?
* What are the top 10 most popular artists?

Since the original data about songs and user behaviour is only available in JSON files and potentially has a large volume it can not be easily analyzed regarding those questions. The data is therefore migrated to a redshift data warehouse using an ETL piepline to allow for the described kind of queries.

## Sample Queries
### What are the top 5 songs played most often?
```SQL
SELECT s.song_id,s.title,count(*) as count
FROM songplays sp
JOIN songs s ON (sp.song_id=s.song_id)
GROUP BY s.song_id, s.title
ORDER BY count desc
LIMIT 5;
```

| Song ID          | Song title                                           | Number of plays |
|------------------|------------------------------------------------------|-----------------|
|SOBONKR12A58A7A7E0|	You're The One	                                  |    37           |
|SOHTKMO12AB01843B0|	Catch You Baby (Steve Pitron & Max Sanna Radio Edit)|	9           |
|SOUNZHU12A8AE47481|	I CAN'T GET STARTED	                              |  9              |
|SOULTKQ12AB018A183|	Nothin' On You [feat. Bruno Mars] (Album Version) |	8               |
|SOLZOBD12AB0185720|	Hey Daddy (Daddy's Home)	                      | 6               |



### What artists are most often played by female users?
```SQL
SELECT a.name,count(*) as count
FROM songplays sp
JOIN users u ON (sp.user_id=u.user_id)
JOIN artists a ON (sp.artist_id=a.artist_id)
WHERE u.gender = 'F'
GROUP BY a.artist_id, a.name
ORDER BY count desc
LIMIT 5;
```

| Artist      | Number of Plays |
|-------------|-----------------|
|Muse	      |35               |
|Dwight Yoakam|	26              |
|Radiohead	  |24               |
|The Smiths	  |24               |
|Lonnie Gordon|	12              |