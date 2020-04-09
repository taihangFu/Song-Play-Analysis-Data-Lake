import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

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


def process_song_data(spark, input_data, output_data, local_data=True):
    # get filepath to song data file
    if local_data:
        song_data = get_files("data/song-data") # for Spark to read all JSON files from a directory i
        #assert(song_data[0]=='/home/workspace/data/song_data/A/B/C/TRABCRU128F423F449.json')
    else:
        song_data = input_data+"" #TODO
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_table")
    df.printSchema()

    # extract columns to create songs table
    # songs: song_id, title, artist_id, year, duration
    songs_table = spark.sql("SELECT song_id,title,artist_id,year,duration \
        FROM song_data_table")
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet("songs.parquet")
    
    # Testing
    # Parquet files can also be used to create a temporary view and then used in SQL statements.
    parquetFile = spark.read.parquet("songs.parquet")
    parquetFile.createOrReplaceTempView("parquetFile")
    songs = spark.sql("SELECT * FROM parquetFile WHERE year = 1969").show()
    
'''
    # extract columns to create artists table
    artists_table = 
    
    # write artists table to parquet files
    artists_table = 
    '''
'''
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table
'''

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
