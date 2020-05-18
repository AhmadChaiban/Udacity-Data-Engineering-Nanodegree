import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def user_feedback(df, message):
    """
        Gives the user some feedback when the tables 
        are being created
        
        :param df: the spark df
        :message: the message to the user
    """
    print('****************************')
    print(message)
    df.printSchema()
    print('****************************')

def getTimeStamp(timestamp):
    """
        Uses the standard pandas timestamp function
        to get the timestamp from an integer value
        
        :param timestamp: string (or integer) value
        :return: timestamp
    """
    return pd.Timestamp(int(timestamp))

def getDateTime(timestamp):
    """
        Converts to datetime from timestamp
        
        :param timestamp: string (or integer) value
        :return: datetime value
    """
    return datetime.fromtimestamp(int(timestamp)/1e3)

def create_spark_session():
    """
        Creates the spark session with the 
        hadoop-aws config
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Gets the data from the s3 bucket and creates the 
        artists and songs tables
        
        :param spark: the spark session
        :param input_data: the basic path for the input data
        :param output_data: the basic path where the data will be written
        :return none:
    
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    song_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", FloatType(), True),
        StructField("year", IntegerType(), False)
    ])
    
    # read song data file
    df_song = spark.read.csv(song_data, schema = song_schema)

    # extract columns to create songs table
    songs_table = df_song.select('song_id',
                        col('title').alias('song_title'), 
                        'artist_id', 
                        'year', 
                        'duration').dropDuplicates()
    
    ## Table created user feedback
    user_feedback(songs_table, 'Created Songs Table')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('Year', 'artist_id').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = artists_table = df_song.select('artist_id',
                        'artist_name', 
                        'artist_location', 
                        'artist_latitude', 
                        'artist_longitude')\
                            .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')
    
    ## Table created user feedback
    user_feedback(artists_table, 'Created Artists Table')



def process_log_data(spark, input_data, output_data):
    """
        Gets the log data from the s3 bucket and creates the 
        users, time and songplays tables
        
        :param spark: the spark session
        :param input_data: the basic path for the input data
        :param output_data: the basic path where the data will be written
        :return none:
    
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'
    
    ## Defining schema
    log_schema = StructType([
        StructField("artist", StringType(), False),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", FloatType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", FloatType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", StringType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), False)
    ])

    # read log data file
    df_log = spark.read.json(log_data, schema = log_schema)
    
    # filter by actions for song plays
    df_log = df_log.select('*').where(df_log.page == 'NextSong')

    # extract columns for users table    
    users_table = df_log.select('userId', 
                        'firstName', 
                        'lastName', 
                        'gender', 
                        'level')\
                            .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')
    
    ## Table created user feedback
    user_feedback(users_table, 'Created Users Table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(getTimeStamp, TimestampType())
    df_log = df_log.withColumn('start_time', get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    get_date_time = udf(getDateTime, DateType())
    df_log = df_log.withColumn('date_time', get_date_time(df_log.ts))
    
    # extract columns to create time table
    time_table =  df_log.select('start_time').dropDuplicates()\
                .withColumn("hour", hour(col('start_time')))\
                .withColumn("day", dayofmonth(col('start_time')))\
                .withColumn("week", weekofyear(col('start_time')))\
                .withColumn("month", month(col('start_time')))\
                .withColumn("year", year(col('start_time')))\
                .withColumn("weekday", dayofweek(col('start_time'))) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')
    
    ## Table created user feedback
    user_feedback(time_table, 'Created time Table')

    # Path for song data
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    ## Defining schema once more
    song_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("duration", FloatType(), True),
        StructField("year", IntegerType(), False)
    ])
    
    ## Reading data
    df_song = spark.read.csv(song_data, schema = song_schema)
    
    ## Renaming columns for join
    df_song = df_song.selectExpr('title as song_title', '*')
    df_log = df_log.selectExpr('artist as artist_name', 'song as song_title','*')

    df_song_log = df_log.join(df_song, on=['song_title', 'artist_name'], how = 'outer')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays = df_song_log.select('start_time', 
                               'userId', 
                               'level', 
                               'song_id', 
                               'artist_id', 
                               'sessionId', 
                               'userAgent', 
                               'location', 
                               'year')\
                                .withColumn("month", month(col('start_time')))\
                                    .dropDuplicates()

    songplays = songplays.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays.write.partitionBy('year', 'month').parquet(output_data + 'songplays/')
    
    ## Table created user feedback
    user_feedback(songplays, 'Created songplays Table')


def main():
    """
        1. Creating the spark session
        2. defining the basic data paths (write and read)
        3. Create the star schema with both functions and 
           write them to the s3 bucket
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-data-lake-project-2187/ExtractedData/"
    output_data = "s3a://udacity-data-lake-project-2187/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
