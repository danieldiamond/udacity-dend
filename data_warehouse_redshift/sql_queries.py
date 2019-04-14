import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
                                (artist varchar(100),
                                auth varchar(10),
                                firstName varchar(32),
                                gender varchar(2),
                                itemInSession int,
                                lastName varchar(32),
                                length numeric,
                                level varchar(16),
                                location varchar(64),
                                method varchar(3),
                                page varchar(16),
                                registration DOUBLE PRECISION,
                                sessionId int,
                                song varchar,
                                status int,
                                ts timestamp,
                                userAgent varchar(140),
                                userId int);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (song_id varchar(18),
                                num_songs int,
                                title varchar(256),
                                artist_name varchar(256),
                                artist_latitude numeric,
                                year int,
                                duration numeric,
                                artist_id varchar,
                                artist_longitude numeric,
                                artist_location varchar(100));""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                         (songplay_id int IDENTITY(0,1) PRIMARY KEY,
                         start_time timestamp,
                         user_id int NOT NULL REFERENCES users(user_id),
                         level varchar,
                         song_id varchar REFERENCES songs(song_id),
                         artist_id varchar REFERENCES artists(artist_id),
                         session_id int,
                         location varchar,
                         user_agent varchar)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                     (user_id int PRIMARY KEY,
                     first_name varchar,
                     last_name varchar,
                     gender varchar,
                     level varchar)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                     (song_id varchar PRIMARY KEY,
                     title varchar,
                     artist_id varchar,
                     year numeric,
                     duration numeric)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                       (artist_id varchar PRIMARY KEY,
                       artist_name varchar,
                       artist_location varchar,
                       artist_latitude numeric,
                       artist_longitude numeric)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                     (start_time timestamp PRIMARY KEY,
                     hour int,
                     day int,
                     week int,
                     month int,
                     year int,
                     weekday int)""")

# STAGING TABLES
staging_events_copy = ("""copy staging_events from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2' FORMAT AS JSON {}
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3'].get('LOG_DATA'),
            config['IAM_ROLE'].get('ARN').strip("'"),
            config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs
from {}
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3'].get('SONG_DATA'),
            config['IAM_ROLE'].get('ARN').strip("'"))

songplay_table_insert = ("""INSERT INTO songplays (start_time,
                         user_id, level, song_id, artist_id, session_id,
                         location, user_agent)
                         SELECT DISTINCT to_timestamp(to_char(e.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                         e.userid, e.level, s.song_id,
                         s.artist_id, e.sessionid, s.artist_location,
                         e.useragent
                         FROM staging_songs s
                         JOIN staging_events e
                         ON s.title = e.song
                         AND s.artist_name = e.artist
                         AND s.duration = e.length;""")

user_table_insert = ("""INSERT INTO users (user_id, first_name,
                     last_name, gender, level)
                     SELECT DISTINCT userid, firstname,
                     lastname, gender, level
                     FROM staging_events
                     WHERE userid IS NOT NULL;
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title,
                     artist_id, year, duration)
                     SELECT DISTINCT song_id, title, artist_id, year, duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name,
                       artist_location,
                       artist_latitude, artist_longitude)
                       SELECT DISTINCT artist_id, artist_name, artist_location,
                       artist_latitude, artist_longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL;
                       """)


time_table_insert = ("""INSERT INTO time (start_time, hour, day,
                     week, month, year, weekday)
                     SELECT DISTINCT ts, extract(hour from ts), extract(day from ts),
                     extract(week from ts), extract(month from ts),
                     extract(year from ts), extract(weekday from ts)
                     FROM staging_events
                     WHERE ts IS NOT NULL;
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
