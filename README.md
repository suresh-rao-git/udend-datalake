# Data Lake 

Sparkify a music streaming startup and having data which are collected from various sources like log
files and user access.

There is need to analysize the data that is generated in order to provide further enhancements to 
Sparkify website making it more user friendly by provideing recomendation as well as to provide reports
to management to see how to make uses sticky and get more users join as members.


# Process

Data for Songs and Events are stored in S3 buckets.  This data needs to be loaded into Spark cluster.


## Processing songs data
Process the songs data and load into dataframe using custom schema. Song table and Artist table are created by 
creating intermediate dataframe and writing the data in parquet format.  

## Processing events data
Process the events data and load into dataframe, filtering for NextSong, creat UDF. User table and Time table are created
and written into parquet format.
Songplay table is created by joing Song and Event dataframe.


In order to do Analytics, we need to transform the raw data in Star/Snowflake schema.  The schema is transformed into
Fact Table:
    SongPlay
Dimension tables:
    Users
    Artists
    Song
    Time
    

#How to handle timestamps

https://community.cloudera.com/t5/Support-Questions/pyspark-convert-unixtimestamp-to-datetime/td-p/187400

https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html

