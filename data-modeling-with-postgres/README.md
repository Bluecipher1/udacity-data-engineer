# Data Modeling with Postgres
This directory contains a solution to the first project of Udacity's *Data Engineer* Nanodegree program. It describes the data workflow Sparkify, a fictitious music streamimg app. The data set being used (not contained in this folder) is a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).

## Usage

### Requirements
The implementation is based on Python 3 which needs to be installed locally. It also requires the modules *psycopg2* and *pandas* to available for import. To run the two notebooks to discover the workflow and table data (*etl.ipynb* and *test.ipynb*, respectively) you need to be able to run a Jupyter notebook.
The song dataset is being extracted from several files the JSON format, transformed and the loaded into a Postgres database. You therefore need a local Postgres installation available on localhost (127.0.0.1) at the standard port (5432).

### Execution
The table creation and ETL pipeline are implemented by two Python scripts which need to be executed in the following order:
```
python create_table.py
python etl.py
```

## Why is the data moved to a database?
The Sparkify data analystics team would like to analyze the listening behaviour od their clients. This includes questions like
* What songs are users listening to?
* How long do they listen to a song on average?
* What are the top 10 favourite songs?
* What are the top 10 most popular artists?

Since the original data about songs and user behaviour is only available in JSON files it can not be easily analyzed regarding those questions. The data is therefore migrated to a Postgres database using an ETL piepline to allow for the described kind of queries.

## Database schema (*create_tables.py*)
The kind ad-hoc queries and aggregations described above can best be realized by a star schema, i.e. fact and dimension tables. In this case the fact (song plays) is linked to several dimensions: songs, artists, users and time. This allows for performant and easy-to-understand joins between the tables to answer all kinds of questions about songplays.

### Songs (dimension)

| Column   | Data type | Comment |
| -------  | --------- | ------- |
| song_id  | varchar   |         |
| title    | varchar   |         |
| artist_id| varchar   |         |
| year     | int       |         |
| duration | numeric   |         |


### Artists (dimension)

| Column   | Data type | Comment |
| -------  | --------- | ------- |
| artist_id| varchar   |         |
| name     | varchar   |         |
| location | varchar   |         |
| latitude | numeric   |         |
| longitude| numeric   |         |

### Users (dimension)

| Column    | Data type | Comment |
| -------   | --------- | ------- |
| user_id   | int       |         |
| first_name| varchar   |         |
| last_name | varchar   |         |
| gender    | varchar   | 'M' or 'F'|
| level     | varchar   | membership type|

### Time (dimension)

| Column     | Data type | Comment |
| -------    | --------- | ------- |
| start_time | timestamp |         |
| hour       | int       |         |
| day        | int       |         |
| week       | int       |         |
| month      | int       |         |
| year       | int       |         |
| weekday    | int       |         |

### Songplays (fact)

| Column      | Data type | Comment |
| -------     | --------- | ------- |
| songplay_id | int       | auto inc|
| start_time  | timestamp | links to *time* table|
| user_id     | int       | links to *users* table |
| song_id     | varchar   | links to *songs* table |
| artist_id   | varchar   | links to *artists* table |
| session_id  | int       |         |
| location    | varchar   |         |
| user_agent  | varchar   | device used to play song  |

### ETL pipleline (*etl.py*)
The ETL pipleine will first process all song files, extract the relevant information and populate the corresponding dimension tables (songs, artists). It then processes the log files containing user events. It filters those events for the action *NextSong*, since this marks the end of playing one song and beginning to play the next one. The data for the users dimension can be directly loaded into the corresponding table. For the time dimension, however, a transformation from the timestamp in milliseconds since the Unix epoch into the correspondings days, weeks etc. is necessary.
Finally, the songplays fact table is populated using the dimension keys for joing those tables as well as some additional data.

## Sample Queries
### What are the top 5 songs played most often?
```SQL
SELECT s.song_id,s.title,count(sp) as count
FROM songplays sp
JOIN songs s ON (sp.song_id=s.song_id)
GROUP BY s.song_id, s.title
ORDER BY count desc
LIMIT 5;
```
|song_id 	|title 	|count|
|-----------|-------|-----|
|SOZCTXZ12AB0182364 	|Setanta matins 	|1|

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
|name |count|
|-----|-----|
|Elena|463  |