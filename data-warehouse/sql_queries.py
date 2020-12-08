import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN=config.get("IAM_ROLE","ARN")
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR(200),
        auth VARCHAR(20),
        firstName VARCHAR(80),
        gender VARCHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR(80),
        length NUMERIC,
        level VARCHAR(20),
        location VARCHAR(200),
        method VARCHAR(10),
        page VARCHAR(20),
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR(200),
        status VARCHAR(10),
        start_time TIMESTAMP,
        userAgent VARCHAR(200),
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR(80),
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR(200),
        artist_name VARCHAR(200),
        song_id VARCHAR(20),
        title VARCHAR(200),
        duration NUMERIC,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(20),
        song_id VARCHAR(20),
        artist_id VARCHAR(20),
        session_id VARCHAR(20),
        location VARCHAR(200),
        user_agent VARCHAR(200)
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR(80),
        last_name VARCHAR(80),
        gender VARCHAR(1),
        level VARCHAR(20)
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(20) PRIMARY KEY,
        title VARCHAR(200) NOT NULL,
        artist_id VARCHAR(80),
        year INTEGER,
        duration NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(20) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        location VARCHAR(200),
        latitude NUMERIC,
        longitude NUMERIC    
    );
""")

time_table_create = ("""
    CREATE TABLE times (
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
    credentials 'aws_iam_role={}' 
    json '{}'
    region 'us-west-2'
    TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT se.start_time, se.userId, se.level, ss.song_id, artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song=ss.title AND se.artist=ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page='NextSong' AND userId NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO times(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM staging_events
    WHERE page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
