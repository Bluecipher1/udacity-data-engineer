import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Creates a Spark session.
    
    Returns: the Spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Loads the song data from JSON files, creates dataframes for songs and artists,
                and writes those dataframes as Parquet files back to S3, where songs are partitioned by year and artist.
                
    Arguments:
        spark: the Spark session
        input_data: S3 bucket to load the song data from
        output_data: S3 bucket to write the transformed data to
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates().cache()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.selectExpr(
        "artist_id", 
        "artist_name as name", 
        "artist_location as location", 
        "artist_latitude as latitude", 
        "artist_longitude as longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: Loads the event data from JSON files and creates dataframes for users, times and songplays. It writes those dataframes
        back to S3 as Parquet files, both songplays and times partitioned by year and month.
                
    Arguments:
        spark: the Spark session
        input_data: S3 bucket to load the event data from
        output_data: S3 bucket to write the transformed data to   
    """    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.option("recursiveFileLookup", "true").json(log_data).drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'").cache()

    # extract columns for users table    
    users_table = df.selectExpr(
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.datetime.fromtimestamp(ms/1000.0))
    spark.udf.register("get_timestamp", get_timestamp)
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: datetime.timestamp(dt))
    spark.udf.register("get_datetime", get_datetime)
    df = df.withColumn("timestamp", get_datetime("start_time"))
    
    # extract columns to create time table
    time_table = df.select(
        "start_time",
        hour("start_time").alias("hour"),
        day("start_time").alias("day"),
        weekofyear("start_time").alias("week"),
        month("start_time").alias("month"),
        year("start_time").alias("year"),
        dayofweek("start_time").alias("weekday").drop_duplicates()
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "times/", mode="overwrite", partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/*/*/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(broadcast(df), df.song == song_df.title & df.artist == song_df.artist_name)\
        .select(
            "start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent"
        ).withColumn("songplay_id", monotonically_increasing_id).drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year", "month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://bluecipher-dend-spark/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
