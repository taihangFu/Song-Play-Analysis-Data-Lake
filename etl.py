import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
#config.read('dl.cfg', encoding='utf-8-sig')
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, run_local=True):
    # get filepath to song data file
    if run_local:
        song_data = get_files("data/song-data") # for Spark to read all JSON files from a directory i
        output_data = ""
    else:
        song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")
    df.printSchema()

    # extract columns to create songs table
    # songs: song_id, title, artist_id, year, duration
    songs_table = spark.sql("SELECT song_id,title,artist_id,year,duration \
        FROM song_data_table\
        WHERE song_id IS NOT NULL").drop_duplicates()
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    # artist_id, name, location, lattitude, longitude
    artists_table = spark.sql("SELECT artist_id , artist_name as name, artist_location as location, artist_latitude as lattitude, artist_longitude as longitude\
        FROM song_data_table\
        WHERE artist_id IS NOT NULL").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")
    
def process_log_data(spark, input_data, output_data, run_local=True):
    # get filepath to log data file
    if run_local:
        log_data = get_files("data/log-data") # for Spark to read all JSON files from a directory i
        output_data = ""
    else:
        log_data = input_data+"log-data/*/*/*.json" #TODO
        
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")
    df.printSchema()

    # extract columns for users table
    # user_id, first_name, last_name, gender, level
    users_table = spark.sql("SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level FROM log_data_table WHERE userId IS NOT NULL").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    # to_timestamp Converts a Column of pyspark.sql.types.StringType or pyspark.sql.types.TimestampType into pyspark.sql.types.DateType 
    time_table = spark.sql("""
                            SELECT 
                            T.start_time as start_time,
                            hour(T.start_time) as hour,
                            dayofmonth(T.start_time) as day,
                            weekofyear(T.start_time) as week,
                            month(T.start_time) as month,
                            year(T.start_time) as year,
                            dayofweek(T.start_time) as weekday
                            FROM
                            (SELECT to_timestamp(log_data_table.ts/1000) as start_time
                            FROM log_data_table 
                            WHERE log_data_table.ts IS NOT NULL
                            ) T
                        """).drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "times")

    # read in song data to use for songplays table
    #song_df = 

    # extract columns from joined song and log datasets to create songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                month(to_timestamp(logT.ts/1000)) as month,
                                year(to_timestamp(logT.ts/1000)) as year,
                                logT.userId as user_id,
                                logT.level as level,
                                songT.song_id as song_id,
                                songT.artist_id as artist_id,
                                logT.sessionId as session_id,
                                logT.location as location,
                                logT.userAgent as user_agent
                                FROM log_data_table logT
                                JOIN song_data_table songT on logT.artist = songT.artist_name and logT.song = songT.title
                            """).drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://myawstwstbucket/"
    
    process_song_data(spark, input_data, output_data, run_local=False)    
    process_log_data(spark, input_data, output_data, run_local=False)


if __name__ == "__main__":
    main()
