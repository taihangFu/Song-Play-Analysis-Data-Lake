# Song-Play-Analysis-Data-Lake

## Task
Built an ETL pipeline that extracts data from S3, processes using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow analysis to continue finding insights.
## Origin Dataset
### Song Dataset
This is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
### Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from a music streaming app based on specified configurations.

The log files in the dataset we'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

## Star Schema for Song Play Analysis after ETL process
Using the song and log datasets, we'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page `NextSong`
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
2. <b>users</b> - users in the app
    * user_id, first_name, last_name, gender, level
3. <b>songs</b> - songs in music database
    * song_id, title, artist_id, year, duration
4. <b>artists</b> - artists in music database
    * artist_id, name, location, lattitude, longitude
5. <b>time</b> - timestamps of records in <b>songplays</b> broken  down into specific units
    * start_time, hour, day, week, month, year, weekday
    
## Instructions
* Spark Submit `etl.py` to read in files from a S3 bucket, process them using Spark and store them as parquet files in a new S3 bucket.
* `etl.py` can be submitted locally or on cloud service such as AWS EMR
* To spark submit `etl.py` on AWS EMR cluster you will have to set up a new EMR cluster on AWS and corresponding security group inbound rule to allow SSH to the EMR master node(hosted in EC2) from your IP address. Then copy the files from your local computer to the EMR node through SCP command before you spark submit the `etl.py` on the EMR node.
  * Set your EMR region to us-west-2 that is where the S3 Bucket located
  
## Sample Queries
Includes in `query.ipynb`
https://github.com/taihangFu/Song-Play-Analysis-Data-Lake/blob/master/query.ipynb
