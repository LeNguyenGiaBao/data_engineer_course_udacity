import configparser
from datetime import datetime
import os
import glob 
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, FloatType, LongType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
        create and config spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
        This function process song data from file to table and save it to s3
    '''
    # get filepath to song data file
    song_data = glob.glob(input_data + 'song_data/*/*/*/*.json')
    
    # data structure 
    schema = StructType() \
      .add("artist_id",StringType(),False) \
      .add("artist_latitude",FloatType(),True) \
      .add("artist_location",StringType(),True) \
      .add("artist_longitude",FloatType(),True) \
      .add("artist_name",StringType(),True) \
      .add("duration",FloatType(),True) \
      .add("num_songs",IntegerType(),True) \
      .add("song_id",StringType(),False) \
      .add("title",StringType(),True) \
      .add("year",IntegerType(),True) 

    # read song data file
    df = spark.read.schema(schema).json(song_data)

    # extract columns to create songs table
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.selectExpr(*song_columns).dropDuplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+'song_table')

    # extract columns to create artists table
    artist_columns = ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = df.selectExpr(*artist_columns).dropDuplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artist_table')


def process_log_data(spark, input_data, output_data):
    '''
        This function process log data from file to table and save it to s3
    '''
    # get filepath to log data file
    log_data = glob.glob(input_data + 'log_data/*.json')

    # data structure 
    schema = StructType() \
      .add("artist",StringType(),True) \
      .add("auth",StringType(),True) \
      .add("firstName",StringType(),True) \
      .add("gender",StringType(),True) \
      .add("itemInSession",IntegerType(),True) \
      .add("lastName",StringType(),True) \
      .add("length",FloatType(),True) \
      .add("level",StringType(),True) \
      .add("location",StringType(),True) \
      .add("method",StringType(),True) \
      .add("page",StringType(),True) \
      .add("registration",StringType(),True) \
      .add("sessionId",IntegerType(),True) \
      .add("song",StringType(),True) \
      .add("status",IntegerType(),True) \
      .add("ts",LongType(),True) \
      .add("userAgent",StringType(),True) \
      .add("userId",StringType(),True) 
    
    # read log data file
    df = spark.read.schema(schema).json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page=='NextSong')

    # extract columns for users table    
    user_columns = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(*user_columns).dropDuplicates(subset=['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users_table')

    # create timestamp column from original timestamp column
    # get_timestamp = udf(lambda x: x//1000)
    df = df.withColumn("timestamp", (col("ts")/1000).cast("timestamp"))
    
    # create datetime column from original timestamp column
    df = df.withColumn("year", year(col("timestamp")))\
        .withColumn("month", month(col("timestamp")))\
        .withColumn("dayofmonth", dayofmonth(col("timestamp")))\
        .withColumn("hour", hour(col("timestamp")))\
        .withColumn("weekofyear", weekofyear(col("timestamp")))\
        .withColumn("weekday", date_format(col("timestamp"), 'u'))
    
    # extract columns to create time table
    time_column = ['ts', 'hour', 'dayofmonth as day', 'weekofyear as week', 'month', 'year', 'weekday']
    time_table = df.selectExpr(*time_column).dropDuplicates(subset=['ts'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"song_table").select(['song_id', 'title', 'artist_id', 'duration'])
    
    # read in artist data to use for songplays table
    artist_df = spark.read.parquet(output_data+"artist_table").select(['name'])

    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = df.select(['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'song', 'length', 'artist']) \
                    .join(song_df, (song_df.title == df.song) & (song_df.duration == df.length)) \
                    .join(artist_df, artist_df.name == df.artist)
    
    songplays_column = ['ts as start_time', 'userId as user_id', 'level', 'song_id', 'artist_id', 'sessionId as session_id', 'location', 'userAgent as user_agent']
    songplays_table = songplays_df.selectExpr(*songplays_column)
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id()) \
                    .withColumn('year', year((col("start_time")/1000).cast("timestamp"))) \
                    .withColumn('month', month((col("start_time")/1000).cast("timestamp")))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data+'songplays_table')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
