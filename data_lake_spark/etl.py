import configparser
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read_file(open('/Users/danieldiamond/.aws/credentials'))

os.environ["AWS_ACCESS_KEY_ID"] = config.get('sparkifyuser',
                                             'AWS_ACCESS_KEY_ID')
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get('sparkifyuser',
                                                 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the song data in the
        filepath (bucket/song_data) to get the song and artist info and
        used to populate the songs and artists dim tables.

    Parameters:
        spark: the cursor object.
        input_path: the path to the bucket containing song data.
        output_path: the path to destination bucket where the parquet files
            will be stored.

    Returns:
        None
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    print('Success: Read song_data from S3')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])
    print('Success: Wrote songs_table to parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude',
                              'artist_longitude').dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists_table',
                                mode='overwrite')
    print('Success: Wrote artists_table to parquet')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the log data in the
        filepath (bucket/log_data) to get the info to populate the
        user, time and song dim tables as well as the songplays fact table.

    Parameters:
        spark: the cursor object.
        input_path: the path to the bucket containing song data.
        output_path: the path to destination bucket where the parquet files
            will be stored.

    Returns:
        None
    """
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    print('Success: Read log_data from S3')

    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    user_table = df.select('userId', 'firstName', 'lastName',
                           'gender', 'level').dropDuplicates()

    # write users table to parquet files
    user_table.write.parquet(f'{output_data}/user_table', mode='overwrite')
    print('Success: Wrote user_table to parquet')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', F.from_unixtime(F.col('ts')/1000))
    print('Success: Convert ts to timestamp')

    # create datetime column from original timestamp column
    time_table = df.select('ts', 'start_time') \
                   .withColumn('year', F.year('start_time')) \
                   .withColumn('month', F.month('start_time')) \
                   .withColumn('week', F.weekofyear('start_time')) \
                   .withColumn('weekday', F.dayofweek('start_time')) \
                   .withColumn('day', F.dayofyear('start_time')) \
                   .withColumn('hour', F.hour('start_time')).dropDuplicates()
    print('Success: Extract DateTime Columns')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f'{output_data}/time_table',
                             mode='overwrite',
                             partitionBy=['year', 'month'])
    print('Success: Wrote time_table to parquet')

    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/A/A/A/*.json'
    song_dataset = spark.read.json(song_data)
    print('Success: Read song_dataset from S3')

    # join & extract cols from song and log datasets to create songplays table
    song_dataset.createOrReplaceTempView('song_dataset')
    time_table.createOrReplaceTempView('time_table')
    df.createOrReplaceTempView('log_dataset')

    songplays_table = spark.sql("""SELECT DISTINCT
                                       l.ts as ts,
                                       t.year as year,
                                       t.month as month,
                                       l.userId as user_id,
                                       l.level as level,
                                       s.song_id as song_id,
                                       s.artist_id as artist_id,
                                       l.sessionId as session_id,
                                       s.artist_location as artist_location,
                                       l.userAgent as user_agent
                                   FROM song_dataset s
                                   JOIN log_dataset l
                                       ON s.artist_name = l.artist
                                       AND s.title = l.song
                                       AND s.duration = l.length
                                   JOIN time_table t
                                       ON t.ts = l.ts
                                   """).dropDuplicates()
    print('Success: SQL Query')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table',
                                  mode='overwrite',
                                  partitionBy=['year', 'month'])
    print('Success: Wrote songplays_table to parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://sparkify-bucket"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
