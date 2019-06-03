import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (event_key INTEGER IDENTITY(0,1), artist_name VARCHAR(MAX), auth VARCHAR(MAX), first_name VARCHAR(MAX), \
    gender VARCHAR(MAX), item_in_session INTEGER, \
    last_name VARCHAR(MAX), duration DOUBLE PRECISION, level VARCHAR(MAX), location VARCHAR(MAX), method VARCHAR(MAX), page VARCHAR(MAX), registration DECIMAL(15,1), session_id INTEGER, \
    title VARCHAR(MAX), status INTEGER, ts TIMESTAMP, user_agent VARCHAR(MAX), user_id VARCHAR(MAX)
    );
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (num_songs INTEGER, artist_id VARCHAR(MAX), latitude DECIMAL, longitude DECIMAL, location VARCHAR(MAX), \
    artist_name VARCHAR(MAX), song_id VARCHAR(MAX), title VARCHAR(MAX), duration DOUBLE PRECISION, year INTEGER
    );
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays 
    (songplay_id BIGINT IDENTITY(0,1), start_time TIMESTAMP, user_id VARCHAR(MAX), \
    level VARCHAR(MAX), song_id VARCHAR(MAX), artist_id VARCHAR(MAX), session_id INTEGER, \
    location VARCHAR(MAX), user_agent VARCHAR(MAX)
    );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users 
    (user_id VARCHAR(MAX), first_name VARCHAR(MAX), last_name VARCHAR(MAX), gender VARCHAR(MAX), level VARCHAR(MAX)
    );
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs
    (song_id VARCHAR(MAX), title VARCHAR(MAX), artist_id VARCHAR(MAX), year INTEGER, duration DOUBLE PRECISION
    );
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists
    (artist_id VARCHAR(MAX), artist_name VARCHAR(MAX), location VARCHAR(MAX), latitude DECIMAL, longitude DECIMAL
    );
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time
    (start_time TIMESTAMP, hour INTEGER, day INTEGER, week INTEGER, month INTEGER, year INTEGER, weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events FROM {}  \
    credentials 'aws_iam_role={}' \
    FORMAT AS JSON {} \
    TIMEFORMAT 'epochmillisecs'; \
    """).format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = (""" COPY staging_songs FROM {} \
    credentials 'aws_iam_role={}' \
    FORMAT AS JSON 'auto';\
    """).format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level FROM staging_events;
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs;
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, location, latitude, longitude FROM staging_songs;
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, extract(hr from ts), extract(day from ts), extract(w from ts), extract(mon from ts), extract(y from ts), extract(weekday from ts) 
    FROM staging_events;
""")

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location , user_agent)
    SELECT se.ts, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id, ss.location, se.user_agent 
    FROM staging_events as se
    JOIN staging_songs as ss 
    ON se.title = ss.title AND se.artist_name = ss.artist_name;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
