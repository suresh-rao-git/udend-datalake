import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StringType , StructField, StructType,IntegerType, DoubleType,StringType ,TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def get_song_schema():
    """
    Method to create spark schema to parse and load the songs data
    
    Args:
    None
    
    Return:
    songs_schema : A StructType object which defines the schema object
    to parse songs_schema.
    
    """
    
    songs_schema = StructType ([
        StructField("num_songs", IntegerType() ),
        StructField("artist_id", StringType() ),
        StructField("artist_latitude", DoubleType() ),
        StructField("artist_longitude", DoubleType() ),
        StructField("artist_location", StringType() ),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType() ), 
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
    ])
    return songs_schema




def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    
    Args:
        spark: spark context object to execute methods against spark cluster
        input_data: location prefix of S3, where input files to process are stored
        output_data: location prefix of S3 where analystics data after post processing
            needs to be stored.
            
    """
    # get filepath to song data file
    song_data = input_data + "/song-data/*/*/*/*.json"
    
    # read song data file
    print ( "reading data from song_data "+ song_data )
    song_json_df = spark.read.json( song_data, schema=get_song_schema())

    
    # extract columns to create songs table
    song_json_df.createOrReplaceTempView( "song_json_df_table")
    
    print ("creating song table dataframe")
    song_table_df = spark.sql( "select distinct song_id, title, artist_id, year, duration \
                                from song_json_df_table order by song_id ")
    
    
    # write songs table to parquet files partitioned by year and artist
    print ( "writing song table datafram in parquet format partitioned by year and artist_id ")
    song_table_df.write.partitionBy( "year", "artist_id").parquet( outputdata + "/song_table" )

    # extract columns to create artists table
    print ("creating artist table dataframe")
    artist_table_df = spark.sql( "select distinct artist_id , artist_name as name , \
                                    artist_location as location, artist_latitude as latitude,  \
                                    artist_longitude as longitude from song_json_df_table")
    
    # write artists table to parquet files
    print ( "writing artist table in parquet format")
    artist_table_df.write.parquet( output_data +  "/artist_table")


def process_log_data(spark, input_data, output_data):
    """
    
    Args:
        spark: spark context object to execute methods against spark cluster
        input_data: location prefix of S3, where input files to process are stored
        output_data: location prefix of S3 where analystics data after post processing
            needs to be stored.
            
    """

    
    # get filepath to log data file
    print ( "loading events json into dataframe ")
    events_json_df = spark.read.json( input_data +  "log-data")
    
    # filter by actions for song plays
    events_json_df = events_json_df.filter( events_json_df.page =='NextSong')
    

    # Create temp view 
    events_json_df.createOrReplaceTempView("events_json_table")
    
    # extract columns for users table
    print ( "creating user table dataframe ")
    user_table_df = spark.sql ( "select distinct userId, firstName as first_name, lastName as last_name , gender, level \
                                    from events_json_table ")
    print ( "writing user table in parquet format")
    user_table_df.write.parquet( output_data + "/user_table")
    
    # create timestamp column from original timestamp column
    @udf(t.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts/1000)
    
    print ( "creating get_datatime udf function")
    # create datetime column from original timestamp column
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S')
    
    print ( "adding new column as timestamp to events dataframe ")
    events_json_df = events_json_df.withColumn( "datetime", get_datetime("ts"))
    # Recreate temp view after adding datetime.
    events_json_df.createOrReplaceTempView("events_json_table")
    
    print ( "creating time table dataframe")
    time_table_df = spark.sql( "select  distinct datetime as start_time , hour(datetime) as hour , \
                                minute(datetime) as minute , day(datetime) as day ,week(datetime) as week, \
                                month(datetime) as month, year(datetime) as year, weekday(datetime) as weekday \
                                from events_json_table")
    
    # write time table to parquet files partitioned by year and month
    print ( "writing time table in parquet format partition by year, month")
    time_table_df.write.partitionBy( "year", "month").parquet( output_data + "/time_table")
    
    
    # extract columns from joined song and log datasets to create songplays table 
    print ( "creating songplay table in dataframe")
    songplay_table_df = spark.sql ("select distinct e.datetime as timestamp,e.userId as userId, e.level as level,\
                                    s.song_id as song_id, s.artist_id as artist_id, e.sessionId as sessionId, \
                                    e.location as location, e.userAgent, year(e.datetime) as year, month(e.datetime) \
                                    as month as user_agent from song_json_df_table s, events_json_table e where e.artist = s.artist_name  ")
    
    # write songplays table to parquet files partitioned by year and month
    print ( "writing songplay table in parquet format partition by year, month")
    songplay_table_df.write.partitionBy("year","month").parquet( output_data +"/songplay_table")
    
    """
    Alternate without using UDFs
    
    time_table_df = spark.sql( "select from_unixtime(ts/1000 , 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp, from_unixtime(ts/1000 , 'HH') as hour ,from_unixtime(ts/1000 , 'dd') as day, from_unixtime(ts/1000 , 'w') as week, from_unixtime(ts/1000 , 'MM') as month , from_unixtime(ts/1000 , 'yyyy') as year, from_unixtime(ts/1000 , 'E') as weekday  from events_json_table ")
    
    song_play_df = spark.sql( "select from_unixtime(e.ts/1000 , 'yyyy-MM-dd HH:mm:ss.SSS') as timestamp , e.userId as userId, e.level as level , s.song_id as song_id, s.artist_id as artist_id, e.sessionId as sessionId, e.location as location, e.userAgent as user_agent from song_json_df_table s, events_json_table e where e.artist = s.artist_name and e.page = 'NextSong' ")
"""    
    
    

def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = input_data + "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
